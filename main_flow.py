# main_flow.py
import subprocess
import sys

def run_script(script_name):
    print(f"--- Executando: {script_name} ---")
    # Usa subprocess.run para executar o script.
    try:
        result = subprocess.run([sys.executable, script_name],
                                capture_output=True, text=True, check=True)
        print(f"Saída de {script_name}:\n{result.stdout}")
        if result.stderr:
            print(f"Erros de {script_name} (se houver):\n{result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"ERRO: O script {script_name} falhou com código {e.returncode}")
        print(f"Saída do erro:\n{e.stderr}")
        sys.exit(e.returncode) # Sai com o código de erro do script falho

if __name__ == "__main__":
    scripts_to_run = [
        "bz_layer.py",
        "sv_layer.py",
        "gd_layer.py"
    ]

    for script in scripts_to_run:
        run_script(script)

    print("--- Todos os scripts executados com sucesso! ---")