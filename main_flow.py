# main_flow.py
import subprocess
import sys
import logging


def run_script(script_name):
    logging.info(f"--- Executing: {script_name} ---")
    try:
        result = subprocess.run([sys.executable, script_name], capture_output=True, text=True, check=True)
        logging.info(f"Output of {script_name}:\n{result.stdout}")
        
        if result.stderr:
            logging.error(f"Errors from {script_name} (if any):\n{result.stderr}")
    
    except subprocess.CalledProcessError as e:
        logging.error(f"ERROR: Script {script_name} failed with code {e.returncode}")
        logging.error(f"Error output:\n{e.stderr}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
    scripts_to_run = [
        "bz_layer.py",
        "sv_layer.py"
    ]

    for script in scripts_to_run:
        run_script(script)

    logging.info("--- All scripts executed successfully! ---")