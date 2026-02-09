import os
import sys

# --- CONFIGURATION ---
# If your robot code uses a specific absolute path, paste it here.
# Otherwise, we will scan the current working directory.
TARGET_PATH = os.getcwd() 
# If the previous script targets a specific subfolder (e.g., "logs"), add it:
# TARGET_PATH = os.path.join(os.getcwd(), "logs")

def audit_drive():
    print(f"🕵️‍♂️  STARTING DRIVE AUDIT")
    print(f"📂  Working Directory (CWD): {os.getcwd()}")
    print(f"🎯  Target Scan Path: {TARGET_PATH}")
    print("-" * 40)

    if not os.path.exists(TARGET_PATH):
        print(f"❌  CRITICAL ERROR: The path '{TARGET_PATH}' does not exist!")
        return

    total_files = 0
    total_dirs = 0
    
    # We use os.walk to verify recursion (scanning inside folders)
    for root, dirs, files in os.walk(TARGET_PATH):
        # Calculate depth to pretty-print hierarchy
        level = root.replace(TARGET_PATH, '').count(os.sep)
        indent = ' ' * 4 * (level)
        
        print(f"{indent}📁  {os.path.basename(root)}/")
        
        sub_indent = ' ' * 4 * (level + 1)
        
        for d in dirs:
            print(f"{sub_indent}➡️  [DIR] {d}")
            total_dirs += 1

        for f in files:
            total_files += 1
            # Check for "protected" keywords to simulate your previous logic
            is_protected = "master-sheet" in f or "secret" in f
            status = "🛡️  PROTECTED" if is_protected else "🗑️  DELETABLE"
            
            # Print file details
            size_bytes = os.path.getsize(os.path.join(root, f))
            print(f"{sub_indent}📄  {f}  ({size_bytes} bytes) -> {status}")

    print("-" * 40)
    print("📊  AUDIT SUMMARY")
    print(f"    Total Directories Found: {total_dirs}")
    print(f"    Total Files Found:       {total_files}")
    
    if total_files <= 1:
        print("\n⚠️   DIAGNOSIS:")
        print("    Python sees almost no files here.")
        print("    1. Are you running the script from the wrong terminal directory?")
        print("    2. Is the 'Robot' mounting the drive to a different path (e.g., /Volumes/Robot)?")

if __name__ == "__main__":
    audit_drive()