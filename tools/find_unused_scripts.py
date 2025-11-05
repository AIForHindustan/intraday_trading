#!/usr/bin/env python3
"""
Find all Python scripts that are not imported anywhere in the codebase.
Uses AST parsing and ripgrep for comprehensive analysis.
"""

import ast
import subprocess
from pathlib import Path
from collections import defaultdict
import sys

def get_all_python_files():
    """Get all Python files using fd"""
    result = subprocess.run(['fd', '-e', 'py', '--type', 'f'], 
                           capture_output=True, text=True, cwd='.')
    files = [Path(f.strip()) for f in result.stdout.strip().split('\n') if f.strip()]
    return [f for f in files if '__pycache__' not in str(f) and '.pyc' not in str(f)]

def build_module_map(files):
    """Build mapping from files to possible module names"""
    file_to_modules = {}
    project_root = Path.cwd()
    
    for py_file in files:
        try:
            # Resolve absolute path
            if not py_file.is_absolute():
                py_file = py_file.resolve()
            
            # Get relative path
            try:
                rel_path = py_file.relative_to(project_root)
            except ValueError:
                # Try with resolved path
                try:
                    rel_path = Path(py_file).relative_to(project_root.resolve())
                except ValueError:
                    # File is not in project root, skip
                    continue
            
            filename_no_ext = rel_path.stem
            
            # Build module variants
            variants = []
            
            # Full module path
            if len(rel_path.parts) > 1:
                full_module = '.'.join(rel_path.parts[:-1] + (filename_no_ext,))
                variants.append(full_module)
            
            # Just filename
            variants.append(filename_no_ext)
            
            # Path-based
            path_str = str(rel_path).replace('.py', '').replace('/', '.').replace('\\', '.')
            variants.append(path_str)
            
            # Add parent modules
            if len(rel_path.parts) > 1:
                for i in range(1, len(rel_path.parts)):
                    parent_module = '.'.join(rel_path.parts[:i])
                    variants.append(parent_module)
            
            file_to_modules[py_file] = list(set(variants))
        except Exception as e:
            continue
    
    return file_to_modules

def extract_imports(py_file):
    """Extract all imports from a Python file using AST"""
    imports = set()
    
    try:
        content = py_file.read_text(encoding='utf-8', errors='ignore')
        tree = ast.parse(content, filename=str(py_file))
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    # Add full name and base name
                    imports.add(alias.name)
                    if '.' in alias.name:
                        imports.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module)
                    if '.' in node.module:
                        imports.add(node.module.split('.')[0])
    except Exception as e:
        pass
    
    return imports

def check_with_ripgrep(py_file, module_variants):
    """Use ripgrep to check if file is imported"""
    for variant in module_variants:
        patterns = [
            f'from {variant}',
            f'import {variant}',
            f'from {variant}.',
            f'import {variant}.',
        ]
        
        for pattern in patterns:
            try:
                result = subprocess.run(
                    ['rg', '-l', pattern, '--type', 'py', '.'],
                    capture_output=True,
                    text=True,
                    cwd='.',
                    timeout=5
                )
                
                if result.returncode == 0:
                    matches = [Path(f.strip()) for f in result.stdout.strip().split('\n') if f.strip()]
                    matches = [m for m in matches if m != py_file and '__pycache__' not in str(m)]
                    
                    if matches:
                        return True
            except:
                continue
    
    return False

def main():
    print("Finding all Python scripts not imported anywhere...")
    print("=" * 80)
    
    # Get all files
    all_files = get_all_python_files()
    print(f"\nFound {len(all_files)} Python files")
    
    # Build module map
    print("Building module name mappings...")
    file_to_modules = build_module_map(all_files)
    print(f"Mapped {len(file_to_modules)} files to module names")
    
    # Extract imports from all files
    print("Extracting imports from files...")
    all_imports = {}
    for py_file in all_files:
        all_imports[py_file] = extract_imports(py_file)
    print(f"Extracted imports from {len(all_imports)} files")
    
    # Check which files are imported
    print("Checking which files are imported...")
    imported_files = set()
    
    # Normalize all file paths for comparison
    def normalize_path(p):
        return str(Path(p).resolve())
    
    file_to_modules_normalized = {}
    for py_file, variants in file_to_modules.items():
        file_to_modules_normalized[normalize_path(py_file)] = (py_file, variants)
    
    all_imports_normalized = {}
    for py_file, imports in all_imports.items():
        all_imports_normalized[normalize_path(py_file)] = imports
    
    for norm_path, (py_file, module_variants) in file_to_modules_normalized.items():
        if norm_path in {normalize_path(f) for f in imported_files}:
            continue
        
        # Check AST imports
        for other_norm_path, imports in all_imports_normalized.items():
            if other_norm_path == norm_path:
                continue
            
            # Check if any import matches any module variant
            for variant in module_variants:
                if variant in imports:
                    imported_files.add(py_file)
                    break
            
            if norm_path in {normalize_path(f) for f in imported_files}:
                break
        
        # If not found in AST, check with ripgrep
        if norm_path not in {normalize_path(f) for f in imported_files}:
            if check_with_ripgrep(py_file, module_variants):
                imported_files.add(py_file)
    
    # Final list - use normalized paths for comparison
    imported_paths_normalized = {normalize_path(f) for f in imported_files}
    not_imported = [f for f in all_files if normalize_path(f) not in imported_paths_normalized]
    
    print(f"\n✅ Files that are imported: {len(imported_files)}")
    print(f"❌ Files not imported: {len(not_imported)}")
    
    # Categorize
    standalone_scripts = []
    library_modules = []
    
    for f in not_imported:
        try:
            content = f.read_text(encoding='utf-8', errors='ignore')
            has_main = '__main__' in content or 'if __name__' in content
            if has_main:
                standalone_scripts.append(f)
            else:
                library_modules.append(f)
        except:
            library_modules.append(f)
    
    print(f"\n📜 Standalone scripts (expected to not be imported): {len(standalone_scripts)}")
    print(f"📚 Library modules (potentially unused): {len(library_modules)}")
    
    # Output results
    if library_modules:
        print("\n" + "=" * 80)
        print("📚 LIBRARY MODULES NOT IMPORTED (potentially unused)")
        print("=" * 80)
        for f in sorted(library_modules):
            try:
                size = f.stat().st_size
                print(f"  {f} ({size:,} bytes)")
            except:
                print(f"  {f}")
    
    if standalone_scripts:
        print("\n" + "=" * 80)
        print("📜 STANDALONE SCRIPTS (expected to not be imported)")
        print("=" * 80)
        for f in sorted(standalone_scripts):
            try:
                size = f.stat().st_size
                print(f"  {f} ({size:,} bytes)")
            except:
                print(f"  {f}")
    
    # Save to file
    output_file = Path("unused_scripts_report.txt")
    with open(output_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("UNUSED SCRIPTS REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Total files analyzed: {len(all_files)}\n")
        f.write(f"Files imported: {len(imported_files)}\n")
        f.write(f"Files not imported: {len(not_imported)}\n")
        f.write(f"Library modules (potentially unused): {len(library_modules)}\n")
        f.write(f"Standalone scripts: {len(standalone_scripts)}\n\n")
        
        if library_modules:
            f.write("\n" + "=" * 80 + "\n")
            f.write("LIBRARY MODULES NOT IMPORTED\n")
            f.write("=" * 80 + "\n")
            for mod in sorted(library_modules):
                f.write(f"{mod}\n")
        
        if standalone_scripts:
            f.write("\n" + "=" * 80 + "\n")
            f.write("STANDALONE SCRIPTS\n")
            f.write("=" * 80 + "\n")
            for script in sorted(standalone_scripts):
                f.write(f"{script}\n")
    
    print(f"\n✅ Report saved to: {output_file}")

if __name__ == '__main__':
    main()

