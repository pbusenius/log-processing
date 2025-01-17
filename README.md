# Log-Processing Framework

## Overview
The Log-Processing Framework is a powerful tool designed to simplify and standardize the processing of log files. It offers a flexible and modular architecture to ingest logs from various sources, transform them into a unified schema, perform analyses and enrichments, and present the results in multiple formats.

## Key Features

### 1. **Supported Log Sources**
The framework supports multiple sources:
- **Zeek**: Network monitoring and security logs
- **Velociraptor**: Forensic and endpoint telemetry data
- **Operating System (OS)**: System logs from Linux, Windows, and macOS

### 2. **Unified Schema**
Regardless of the source, all extracted information is transformed into a unified schema. This enables seamless processing and analysis, independent of the data origin.

### 3. **Analyses**
The framework includes prebuilt analysis modules, such as:
- **Brute-Force Detection**: Detect brute-force attacks based on repeated failed authentication attempts.
- Additional modules can be easily added.

### 4. **Enrichments**
Enhance the extracted data with additional information to increase its utility:
- **GeoIP**: Geolocation of IP addresses
- **Tor Node Detection**: Identification of IP addresses belonging to Tor exit nodes

### 5. **Visualization and Export**
- **Map Visualization**: Display geographic information on an interactive map
- **Export**: Output results in a unified format (e.g., JSON, CSV) for further processing

## Installation
### Prerequisites
- Python 3.8+

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/username/log-processing-framework.git
   cd log-processing-framework
   ```
2. Create and activate a virtual environment:
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
3. Run processing:
   ```bash
   uv run processor.py
   ```

## Usage
### Example Usage
1. **Load Log Data**
   ```python
    from src.analysis import ssh
    from src.analysis import http
    from src.source.os import ssh as ssh_os_source
    from src.source.os import http as http_os_source
    from src.source.zeek import ssh as ssh_zeek_source
    from src.source.velociraptor import ssh as ssh_velociraptor_source
    from src.enrichment import ip
    from src.visualization import map
    from src.export import timesketch

    zeek_df = ssh_zeek_source.open_log(args.file)
    os_df = ssh_os_source.open_log("data/auth.log")
    velo_df = ssh_velociraptor_source.open_log("data/auth_velociraptor.log")
    http_df = http_os_source.open_log("data/access.log")
   ```

2. **Run Analyses**
   ```python
    df_brute_force = ssh.brute_force_detection(zeek_df)
    df_common_domain = http.get_common_domains(http_df)
    df_uncommon_domain = http.get_uncommon_domains(http_df)
   ```

3. **Enrich Data**
   ```python
    df_brute_force = ip.city_information(df_brute_force)
    df_brute_force = ip.country_information(df_brute_force)
    df_brute_force = ip.asn_information(df_brute_force)
    df_brute_force = ip.location_information(df_brute_force)
   ```

4. **Export Results**
   ```python
    timesketch.as_json(df_brute_force, "brute_force.jsonl")
    timesketch.as_csv(df_brute_force, "brute_force.csv")
   ```

5. **Map Visualization**
   ```python
   m = map.points(df_brute_force)
   map.add_line(df_brute_force, m)
   map.open_in_browser(m)
   ```

## Extensibility
The framework is modular and allows easy integration of new data sources, analyses, and enrichments. You can write your own modules and integrate them into the framework.


## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

