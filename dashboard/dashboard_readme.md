# Power consumption Dashboard

## Dashboard set up
1. make directories to hold the codes
    ```
    mkdir -p [path]/dashboard/logs
    mkdir -p [path]/dashboard/venv

    cd [path]/dashboard
    ```
2. put `requirements.txt` in `[path]/dashboard`

3. Download and activate python
    ```
    python3 -m venv venv/

    source venv/bin/activate

    # Install tools
    pip install -r requirements.txt
    ```
3. put `app.py` into `[path]/dashboard`
4. execute the command to run the dashboard
    ```
    cd [path]/dashboard
    source venv/bin/activate

    streamlit run app.py --server.address 127.0.0.1
    ```
5. SSH Tunnel the connection. **In a new terminal, run:**
    ```
    ssh -L 8501:localhost:8501 [user]@[ip]
    ```

6. Access the dashboard via `http://localhost:8501`