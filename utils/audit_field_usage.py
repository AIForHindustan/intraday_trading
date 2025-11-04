#!/usr/bin/env python3
"""
Field Usage Auditor
===================

Systematically identifies:
1. Scripts using mismatched field names (hardcoded legacy names instead of canonical)
2. Scripts overriding field mapping logic (bypassing yaml_field_loader)

Usage:
    python utils/audit_field_usage.py
    python utils/audit_field_usage.py --fix  # Show suggested fixes
"""

import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any
import json
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from utils.yaml_field_loader import (
        get_redis_session_fields,
        get_calculated_indicator_fields,
        get_yaml_config,
        resolve_session_field,
    )
except ImportError:
    print("❌ Cannot import yaml_field_loader. Run from project root.")
    sys.exit(1)


class FieldUsageAuditor:
    """Audit field name usage across codebase"""
    
    def __init__(self):
        self.project_root = project_root
        self.issues = {
            'hardcoded_legacy_fields': defaultdict(list),  # field -> [(file, line, context)]
            'bypassing_field_mapper': defaultdict(list),   # file -> [(line, reason)]
            'direct_dict_access': defaultdict(list),       # field -> [(file, line)]
        }
        
        # Load canonical mappings
        self.canonical_fields = self._load_canonical_fields()
        self.legacy_aliases = self._load_legacy_aliases()
        
    def _load_canonical_fields(self) -> Dict[str, Set[str]]:
        """Load canonical field names from YAML"""
        canonical = {
            'session_fields': set(),
            'indicator_fields': set(),
            'websocket_fields': set(),
        }
        
        try:
            config = get_yaml_config()
            
            # Redis session fields (canonical values)
            session_fields = config.get('redis_session_fields', {})
            canonical['session_fields'] = set(session_fields.values())
            
            # Calculated indicator fields
            indicator_fields = config.get('calculated_indicator_fields', {})
            canonical['indicator_fields'] = set(indicator_fields.values())
            
            # WebSocket core fields
            websocket_fields = config.get('websocket_core_fields', {})
            canonical['websocket_fields'] = set(websocket_fields.values())
            
        except Exception as e:
            print(f"⚠️ Error loading canonical fields: {e}")
        
        return canonical
    
    def _load_legacy_aliases(self) -> Dict[str, str]:
        """Load legacy aliases that should be replaced"""
        aliases = {}
        
        try:
            config = get_yaml_config()
            
            # Session field aliases (key is alias, value is canonical)
            session_fields = config.get('redis_session_fields', {})
            for alias, canonical in session_fields.items():
                if alias != canonical:
                    aliases[alias] = canonical
            
        except Exception as e:
            print(f"⚠️ Error loading legacy aliases: {e}")
        
        return aliases
    
    def find_python_files(self) -> List[Path]:
        """Find all Python files in project"""
        python_files = []
        
        exclude_dirs = {'.venv', '__pycache__', '.git', 'node_modules', 'legacy_unused_scripts', 
                       'docs', 'bayesian_model', 'legacy_documentation'}
        
        for py_file in self.project_root.rglob('*.py'):
            # Skip excluded directories
            parts = py_file.parts
            if any(exclude in parts for exclude in exclude_dirs):
                continue
            
            python_files.append(py_file)
        
        return sorted(python_files)
    
    def audit_file(self, file_path: Path) -> Dict[str, Any]:
        """Audit a single file for field usage issues"""
        issues_in_file = {
            'hardcoded_legacy': [],
            'bypassing_mapper': [],
            'direct_access': [],
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
        except Exception as e:
            print(f"⚠️ Error reading {file_path}: {e}")
            return issues_in_file
        
        # Skip if it's the field loader itself
        if 'yaml_field_loader' in str(file_path):
            return issues_in_file
        
        # Check for hardcoded legacy field names
        for line_num, line in enumerate(lines, 1):
            # Pattern 1: Direct field access in dict (data['field'], tick['field'], etc.)
            dict_access_pattern = r"\[['\"](\w+)['\"]\]"
            matches = re.finditer(dict_access_pattern, line)
            
            for match in matches:
                field_name = match.group(1)
                
                # Check if it's a legacy alias that should use canonical
                if field_name in self.legacy_aliases:
                    canonical = self.legacy_aliases[field_name]
                    context = self._get_line_context(lines, line_num, 2)
                    issues_in_file['hardcoded_legacy'].append({
                        'line': line_num,
                        'field': field_name,
                        'canonical': canonical,
                        'context': context,
                    })
        
        # Check if file uses yaml_field_loader
        uses_field_loader = any(
            'yaml_field_loader' in line or 
            'apply_field_mapping' in line or
            'resolve_session_field' in line or
            'normalize_session_record' in line
            for line in lines
        )
        
        # Check for field accesses without using field loader
        if not uses_field_loader:
            # Look for common field access patterns
            field_access_patterns = [
                r"\.get\(['\"](\w+)['\"]\)",  # data.get('field')
                r"\[['\"](\w+)['\"]\]",        # data['field']
                r"=.*\['(\w+)'\]",             # field = data['field']
            ]
            
            suspect_fields = set()
            for line in lines:
                for pattern in field_access_patterns:
                    matches = re.findall(pattern, line)
                    for field in matches:
                        # Check if it's a known legacy field
                        if field in self.legacy_aliases or field in ['volume_traded', 'last_quantity']:
                            suspect_fields.add(field)
            
            if suspect_fields:
                issues_in_file['direct_access'] = list(suspect_fields)
        
        # Check for field mapping overrides
        override_patterns = [
            r"field.*mapping.*override",
            r"bypass.*field.*mapper",
            r"hardcode.*field",
            r"manual.*field.*mapping",
        ]
        
        for line_num, line in enumerate(lines, 1):
            for pattern in override_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    issues_in_file['bypassing_mapper'].append({
                        'line': line_num,
                        'context': self._get_line_context(lines, line_num, 1),
                    })
        
        return issues_in_file
    
    def _get_line_context(self, lines: List[str], line_num: int, context_lines: int = 2) -> str:
        """Get context around a line"""
        start = max(0, line_num - context_lines - 1)
        end = min(len(lines), line_num + context_lines)
        context = lines[start:end]
        return '\n'.join(f"{start + i + 1:4d}: {line}" for i, line in enumerate(context))
    
    def run_audit(self) -> Dict[str, Any]:
        """Run full audit across codebase"""
        print("🔍 Starting field usage audit...")
        print(f"📁 Project root: {self.project_root}")
        print()
        
        python_files = self.find_python_files()
        print(f"📊 Found {len(python_files)} Python files to audit")
        print()
        
        all_issues = {
            'files_checked': len(python_files),
            'files_with_issues': 0,
            'hardcoded_legacy_fields': defaultdict(list),
            'files_bypassing_mapper': defaultdict(list),
            'files_with_direct_access': defaultdict(list),
        }
        
        for file_path in python_files:
            issues = self.audit_file(file_path)
            
            has_issues = False
            
            # Collect hardcoded legacy fields
            if issues['hardcoded_legacy']:
                has_issues = True
                rel_path = file_path.relative_to(self.project_root)
                for issue in issues['hardcoded_legacy']:
                    all_issues['hardcoded_legacy_fields'][issue['field']].append({
                        'file': str(rel_path),
                        'line': issue['line'],
                        'canonical': issue['canonical'],
                        'context': issue['context'],
                    })
            
            # Collect bypassing mapper
            if issues['bypassing_mapper']:
                has_issues = True
                rel_path = file_path.relative_to(self.project_root)
                all_issues['files_bypassing_mapper'][str(rel_path)] = issues['bypassing_mapper']
            
            # Collect direct access
            if issues['direct_access']:
                has_issues = True
                rel_path = file_path.relative_to(self.project_root)
                all_issues['files_with_direct_access'][str(rel_path)] = issues['direct_access']
            
            if has_issues:
                all_issues['files_with_issues'] += 1
        
        return all_issues
    
    def generate_report(self, issues: Dict[str, Any]) -> str:
        """Generate human-readable report"""
        report = []
        report.append("=" * 80)
        report.append("FIELD USAGE AUDIT REPORT")
        report.append("=" * 80)
        report.append("")
        report.append(f"Files checked: {issues['files_checked']}")
        report.append(f"Files with issues: {issues['files_with_issues']}")
        report.append("")
        
        # Hardcoded legacy fields
        if issues['hardcoded_legacy_fields']:
            report.append("=" * 80)
            report.append("1. HARDCODED LEGACY FIELD NAMES")
            report.append("=" * 80)
            report.append("")
            report.append("These files use legacy field names instead of canonical names.")
            report.append("Fix: Use yaml_field_loader.resolve_session_field() or canonical names.")
            report.append("")
            
            for field, occurrences in sorted(issues['hardcoded_legacy_fields'].items()):
                canonical = occurrences[0]['canonical']
                report.append(f"\n❌ Field: '{field}' (should be '{canonical}')")
                report.append(f"   Found in {len(occurrences)} locations:")
                report.append("")
                
                for occ in occurrences[:10]:  # Limit to first 10 per field
                    report.append(f"   📄 {occ['file']}:{occ['line']}")
                    report.append(f"      {occ['context'].split(chr(10))[0]}")
                    report.append("")
                
                if len(occurrences) > 10:
                    report.append(f"   ... and {len(occurrences) - 10} more occurrences")
                    report.append("")
        
        # Files bypassing mapper
        if issues['files_bypassing_mapper']:
            report.append("=" * 80)
            report.append("2. FILES BYPASSING FIELD MAPPER")
            report.append("=" * 80)
            report.append("")
            report.append("These files contain code that might override field mapping logic.")
            report.append("")
            
            for file_path, occurrences in sorted(issues['files_bypassing_mapper'].items()):
                report.append(f"📄 {file_path}")
                for occ in occurrences:
                    report.append(f"   Line {occ['line']}: {occ['context'].split(chr(10))[0]}")
                report.append("")
        
        # Files with direct access
        if issues['files_with_direct_access']:
            report.append("=" * 80)
            report.append("3. FILES WITH DIRECT FIELD ACCESS (NO FIELD LOADER)")
            report.append("=" * 80)
            report.append("")
            report.append("These files access fields directly without using yaml_field_loader.")
            report.append("They might be using legacy field names.")
            report.append("")
            
            for file_path, fields in sorted(issues['files_with_direct_access'].items()):
                report.append(f"📄 {file_path}")
                report.append(f"   Suspect fields: {', '.join(sorted(fields))}")
                report.append("")
        
        if not any([issues['hardcoded_legacy_fields'], 
                   issues['files_bypassing_mapper'], 
                   issues['files_with_direct_access']]):
            report.append("✅ No field usage issues found!")
            report.append("")
        
        report.append("=" * 80)
        report.append("AUDIT COMPLETE")
        report.append("=" * 80)
        
        return '\n'.join(report)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Audit field name usage across codebase')
    parser.add_argument('--fix', action='store_true', help='Show suggested fixes')
    parser.add_argument('--output', help='Output file for report (default: stdout)')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    
    args = parser.parse_args()
    
    auditor = FieldUsageAuditor()
    issues = auditor.run_audit()
    
    if args.json:
        output = json.dumps(issues, indent=2)
    else:
        output = auditor.generate_report(issues)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
        print(f"✅ Report written to {args.output}")
    else:
        print(output)


if __name__ == '__main__':
    main()

