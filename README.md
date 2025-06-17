# Veolab Sync Service

This Python service synchronizes data between the **Veolab** database and **IGEO** software using RabbitMQ queues.

## Features

- Listens to and processes incoming data from the `analiticasRecibidas` queue.
- Sends finalized reports to the `analiticasRealizadas` queue.
- Processes confirmations from the `resultadoAnaliticasRealizadas` queue.
- Handles per-company configuration stored in the database.

## Requirements

- Python 3.9+
- Accessible RabbitMQ server
- Veolab database connection

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/dabizspuch/veolabserver.git
   cd veolabserver
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure a `config.ini` file exists under `src/database/` with the database configuration.

## Usage

Navigate to the `src/` directory and run the module:
```bash
cd src
python -m veolabserver
```

## Project Structure

```
veolabserver/
├── icons/
│   ├── veolab.ico
├── src/
│   ├── veolabserver/
│   │   ├── database/
│   │   │   ├── config.ini  # (Git ignored)
│   │   │   ├── database_config.py
│   │   │   ├── database_veolab.py
│   │   ├── veolabserver.py
├── README.md
├── pyproject.toml
├── requirements.txt
```

## Notes

- The `config.ini` file is ignored by Git for security reasons.
- Recommended to run this service as a background process or in a Docker container for continuous operation.

## License

MIT