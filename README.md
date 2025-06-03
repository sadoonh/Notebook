# Notebook Application

A NiceGUI-based notebook application for SQL and Python execution.

## Setup and Launch

1.  **Clone the Repository:**
    Open your terminal or command prompt and run:
    ```
    git clone https://github.com/your_username/DataNotebookApp.git
    cd DataNotebookApp
    ```

2.  **Run the Launch Script:**

    *   **For Windows Users:**
        Double-click the `launch_win.bat` file.
        Alternatively, open a Command Prompt in the `DataNotebookApp` directory and run:
        ```cmd
        launch_win.bat
        ```

    *   **For Linux and macOS Users:**
        Open a terminal in the `DataNotebookApp` directory and run:
        ```bash
        chmod +x launch_linux_mac.sh  # Make executable (only need to do this once)
        ./launch_linux_mac.sh
        ```

    The launch script will:
    *   Check for Python.
    *   Create a Python virtual environment named `venv` (if it doesn't already exist).
    *   Activate the virtual environment.
    *   Install all necessary Python dependencies from `requirements.txt`.
    *   Launch the DataNotebook application.

    Your web browser should automatically open to the application (usually at `http://localhost:8080`).


## Usage

*   The application will store its configuration (like saved database credentials and the default working directory) in `C:\Users\<YourUser>\DataNotebookRoot` (Windows) or `~/.DataNotebookRoot` (Linux/macOS - actually it will be `~/DataNotebookRoot` due to `Path.home()`).
*   Notebook files (`.dnb`) can be saved and loaded.
*   Connect to your PostgreSQL database via the "Connect" button.
