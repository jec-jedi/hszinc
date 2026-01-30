#!/usr/bin/python
# -*- coding: utf-8 -*-
# Fast Zinc Grid Parser - optimized for large grids
# (C) 2026
#
# vim: set ts=4 sts=4 et tw=78 sw=4 si:

import datetime
import re
from typing import Any, Dict, List, Optional, Tuple

import iso8601

from .datatypes import Quantity, Coordinate, Uri, Bin, MARKER, NA, REMOVE, Ref, XStr
from .grid import Grid
from .sortabledict import SortableDict
from .version import Version
from .zoneinfo import timezone


# Regex patterns for fast parsing
VERSION_RE = re.compile(r'^ver:"([^"]+)"')
META_ITEM_RE = re.compile(r'(\w+)(?::(.+?))?(?:\s|$)')
COORD_RE = re.compile(r'C\(([^,]+),([^)]+)\)')
REF_RE = re.compile(r'@([\w:.\-~]*)(?: "([^"]+)")?')
BIN_RE = re.compile(r'Bin\(([^\)]*)\)')
XSTR_RE = re.compile(r'(\w+)\("([^"]*)"\)')
DATETIME_RE = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)(?: (\w+))?')
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
TIME_RE = re.compile(r'^(\d{2}:\d{2}:\d{2}(?:\.\d+)?)$')
NUMBER_RE = re.compile(r'^([+-]?(?:\d+_)*\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)([\w%_/$\u0080-\uffff]+)?$')
URI_RE = re.compile(r'^`([^`]*)`$')
STR_RE = re.compile(r'^"((?:[^"\\]|\\[bfnrt\\"$]|\\[uU][0-9a-fA-F]{4})*)"$')


def _unescape(s: str, uri: bool = False) -> str:
    """Fast string unescaping."""
    if '\\' not in s:
        return s
    
    result = []
    i = 0
    while i < len(s):
        if s[i] == '\\' and i + 1 < len(s):
            esc_c = s[i + 1]
            if esc_c in ('u', 'U') and i + 5 < len(s):
                result.append(chr(int(s[i+2:i+6], 16)))
                i += 6
                continue
            elif esc_c == 'b':
                result.append('\b')
            elif esc_c == 'f':
                result.append('\f')
            elif esc_c == 'n':
                result.append('\n')
            elif esc_c == 'r':
                result.append('\r')
            elif esc_c == 't':
                result.append('\t')
            else:
                if uri and esc_c == '#':
                    result.append('\\')
                result.append(esc_c)
            i += 2
        else:
            result.append(s[i])
            i += 1
    return ''.join(result)


def _parse_scalar(value: str) -> Any:
    """Parse a single scalar value."""
    value = value.strip()
    
    if not value:
        return None
    
    # Quick check for complex types that need slow parser
    if value.startswith('[') or value.startswith('{') or value.startswith('<<'):
        raise ValueError("Complex type - use slow parser")
    
    # Singletons
    if value == 'M':
        return MARKER
    if value == 'N':
        return None
    if value == 'NA':
        return NA
    if value == 'R':
        return REMOVE
    if value == 'T':
        return True
    if value == 'F':
        return False
    
    # Special numbers
    if value == 'INF':
        return float('inf')
    if value == '-INF':
        return float('-inf')
    if value == 'NaN':
        return float('nan')
    
    # Coordinate
    if value.startswith('C('):
        m = COORD_RE.match(value)
        if m:
            return Coordinate(float(m.group(1)), float(m.group(2)))
    
    # Reference
    if value.startswith('@'):
        m = REF_RE.match(value)
        if m:
            ref_id = m.group(1)
            ref_dis = m.group(2) if m.lastindex >= 2 else None
            return Ref(ref_id, ref_dis)
    
    # Bin
    if value.startswith('Bin('):
        m = BIN_RE.match(value)
        if m:
            return Bin(m.group(1))
    
    # XStr
    if '(' in value and value.endswith(')') and not value.startswith('C('):
        m = XSTR_RE.match(value)
        if m:
            return XStr(m.group(1), _unescape(m.group(2)))
    
    # URI
    if value.startswith('`'):
        m = URI_RE.match(value)
        if m:
            return Uri(_unescape(m.group(1), uri=True))
    
    # String
    if value.startswith('"'):
        m = STR_RE.match(value)
        if m:
            return _unescape(m.group(1))
    
    # DateTime
    if 'T' in value and (value[0].isdigit() or value[0] == '-'):
        m = DATETIME_RE.match(value)
        if m:
            dt_str = m.group(1)
            tz_name = m.group(2) if m.lastindex >= 2 else None
            
            dt = iso8601.parse_date(dt_str.upper())
            
            if tz_name and dt.tzinfo is not None:
                try:
                    tz = timezone(tz_name)
                    return dt.astimezone(tz)
                except:
                    pass
            return dt
    
    # Date
    if DATE_RE.match(value):
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()
    
    # Time
    m = TIME_RE.match(value)
    if m:
        time_str = m.group(1)
        fmt = '%H:%M:%S.%f' if '.' in time_str else '%H:%M:%S'
        return datetime.datetime.strptime(time_str, fmt).time()
    
    # Number (with optional unit)
    m = NUMBER_RE.match(value)
    if m:
        num_str = m.group(1).replace('_', '')
        unit = m.group(2) if m.lastindex >= 2 else None
        num = float(num_str)
        if unit:
            return Quantity(num, unit=unit)
        return num
    
    return value


def _split_csv_values(line: str) -> List[str]:
    """Split a line by commas, handling quoted strings."""
    values = []
    current = []
    in_string = False
    in_uri = False
    escape_next = False
    paren_depth = 0
    
    i = 0
    while i < len(line):
        c = line[i]
        
        if escape_next:
            current.append(c)
            escape_next = False
            i += 1
            continue
        
        if c == '\\' and (in_string or in_uri):
            current.append(c)
            escape_next = True
            i += 1
            continue
        
        if c == '"' and not in_uri:
            in_string = not in_string
            current.append(c)
        elif c == '`' and not in_string:
            in_uri = not in_uri
            current.append(c)
        elif c == '(' and not in_string and not in_uri:
            paren_depth += 1
            current.append(c)
        elif c == ')' and not in_string and not in_uri:
            paren_depth -= 1
            current.append(c)
        elif c == ',' and not in_string and not in_uri and paren_depth == 0:
            values.append(''.join(current).strip())
            current = []
        else:
            current.append(c)
        
        i += 1
    
    if current or (line and line[-1] == ','):
        values.append(''.join(current).strip())
    
    return values


def _parse_meta(meta_str: str) -> SortableDict:
    """Parse metadata from a line."""
    meta = SortableDict()
    if not meta_str:
        return meta
    
    # Check for invalid tag names (must start with lowercase letter)
    # If we see something like "ThisIsNotATag", it's invalid
    if meta_str and meta_str[0].isupper():
        raise ValueError("Invalid metadata - tag must start with lowercase letter")
    
    # Simple approach: split by spaces, handle key:value pairs
    items = []
    current_key = None
    current_val = []
    in_quote = False
    in_uri = False
    escape_next = False
    
    i = 0
    while i < len(meta_str):
        c = meta_str[i]
        
        if escape_next:
            current_val.append(c)
            escape_next = False
            i += 1
            continue
        
        if c == '\\':
            current_val.append(c)
            escape_next = True
            i += 1
            continue
        
        if c == '"' and not in_uri:
            in_quote = not in_quote
            current_val.append(c)
        elif c == '`' and not in_quote:
            in_uri = not in_uri
            current_val.append(c)
        elif c == ':' and not in_quote and not in_uri and current_key is None:
            # This is a key:value separator
            current_key = ''.join(current_val).strip()
            current_val = []
        elif c == ' ' and not in_quote and not in_uri:
            # Space separates items
            val_str = ''.join(current_val).strip()
            if val_str:
                if current_key:
                    items.append((current_key, val_str))
                    current_key = None
                else:
                    items.append(val_str)
            current_val = []
        else:
            current_val.append(c)
        
        i += 1
    
    # Handle last item
    val_str = ''.join(current_val).strip()
    if val_str:
        if current_key:
            items.append((current_key, val_str))
        else:
            items.append(val_str)
    
    # Convert to dict
    for item in items:
        if isinstance(item, tuple):
            key, val = item
            # Validate tag name (lowercase letter followed by alphanumeric/underscore)
            if not re.match(r'^[a-z][a-zA-Z0-9_]*$', key):
                raise ValueError(f"Invalid tag name: {key}")
            meta[key] = _parse_scalar(val)
        else:
            # Validate tag name
            if not re.match(r'^[a-z][a-zA-Z0-9_]*$', item):
                raise ValueError(f"Invalid tag name: {item}")
            meta[item] = MARKER
    
    return meta


def parse_grid_fast(grid_str: str) -> Grid:
    """
    Fast parser for simple Zinc grids (CSV-like format).
    This handles the common case of grids without nested structures.
    Raises ValueError for complex grids that need the slow parser.
    """
    # Quick check for complex features
    if '<<' in grid_str or '{' in grid_str:
        # Likely has nested grids or dicts - use slow parser
        raise ValueError("Complex grid - use slow parser")
    
    # Check for lists in first 200 chars (header area)
    header_sample = grid_str[:min(200, len(grid_str))]
    if '[' in header_sample:
        raise ValueError("Complex grid - use slow parser")
    
    # Check for timezone names in metadata (complex datetime parsing)
    # This is indicated by datetime patterns followed by single words
    lines = grid_str.split('\n', 2)
    if len(lines) > 0:
        first_line = lines[0]
        # If we see a datetime with +/-HH:MM followed by a word, it's complex
        if re.search(r'T\d{2}:\d{2}:\d{2}[^,\n]*[+-]\d{2}:\d{2}\s+[A-Z][a-zA-Z_]+', first_line):
            raise ValueError("Complex metadata with timezone names - use slow parser")
        # Check for escaped backticks in URIs which need complex parsing
        if '\\`' in first_line:
            raise ValueError("Complex URI escaping - use slow parser")
    
    lines = grid_str.split('\n')
    if not lines:
        raise ValueError("Empty grid")
    
    line_idx = 0
    
    # Parse version and grid metadata (line 1)
    ver_match = VERSION_RE.match(lines[line_idx])
    if not ver_match:
        raise ValueError(f"Invalid grid: missing version on line 1")
    
    version = Version(ver_match.group(1))
    meta_str = lines[line_idx][ver_match.end():].strip()
    grid_meta = _parse_meta(meta_str)
    # Don't add version to grid_meta, it's handled separately
    line_idx += 1
    
    # Parse column headers (line 2)
    if line_idx >= len(lines):
        raise ValueError("Invalid grid: missing column headers")
    
    col_line = lines[line_idx]
    
    # Check for invalid column metadata (uppercase tag names)
    if re.search(r'\s[A-Z][A-Z]+', col_line):
        raise ValueError("Invalid column metadata - use slow parser")
    
    col_parts = _split_csv_values(col_line)
    
    columns = []
    for col_str in col_parts:
        col_str = col_str.strip()
        if ' ' in col_str:
            # Has metadata
            parts = col_str.split(' ', 1)
            col_name = parts[0]
            col_meta = _parse_meta(parts[1])
        else:
            col_name = col_str
            col_meta = {}
        columns.append((col_name, col_meta))
    
    line_idx += 1
    
    # Create grid
    grid = Grid(version=version, metadata=dict(grid_meta), columns=columns)
    
    # Parse rows
    col_names = [col[0] for col in columns]
    while line_idx < len(lines):
        line = lines[line_idx].strip()
        if not line:
            line_idx += 1
            continue
        
        # Check for escaped backticks in row data
        if '\\`' in line:
            raise ValueError("Complex URI escaping in row - use slow parser")
        
        values = _split_csv_values(line)
        
        # Validate that row values look parseable
        # Check for unquoted non-scalar values
        for val in values:
            val = val.strip()
            if val and not val[0] in ('"', '`', '@', 'M', 'N', 'T', 'F', 'R', 'C', 'B') and not val[0].isdigit() and val[0] not in ('-', '+'):
                # Check if it's a valid identifier (for XStr or special values)
                if not re.match(r'^[a-zA-Z_]\w*\(', val) and val not in ('INF', '-INF', 'NaN', 'NA'):
                    # Unquoted string that's not a recognized type - malformed
                    raise ValueError(f"Malformed value in row: {val}")
        
        # Pad with None if row has fewer values than columns
        while len(values) < len(col_names):
            values.append('')
        
        # Parse each value
        row_data = {}
        for i, col_name in enumerate(col_names):
            if i < len(values):
                row_data[col_name] = _parse_scalar(values[i])
            else:
                row_data[col_name] = None
        
        grid.append(row_data)
        line_idx += 1
    
    return grid


def parse_grid(grid_str: str, parseAll: bool = True) -> Grid:
    """
    Parse a Zinc grid string. Uses fast parser for simple grids,
    falls back to pyparsing for complex cases.
    """
    # Try fast parser first
    try:
        return parse_grid_fast(grid_str)
    except Exception as e:
        # Fall back to the original pyparsing implementation
        # Import here to avoid circular dependency
        from .zincparser import parse_grid as parse_grid_slow
        return parse_grid_slow(grid_str, parseAll=parseAll)


def parse_scalar(scalar_data: str, version: Version) -> Any:
    """Parse a scalar value (fast implementation)."""
    return _parse_scalar(scalar_data)
