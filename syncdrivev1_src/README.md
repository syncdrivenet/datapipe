# SyncDrive V1

Vehicle drive recording processor with web UI for batch processing multi-source driving session data.

## Features

- **Multi-camera video processing** - Concatenates video segments with hardware acceleration (VideoToolbox on macOS)
- **CAN bus parsing** - Decodes raw CAN data into human-readable signals (wheel speed, brake, steering, RPM, temperature)
- **Sensor data support** - Processes phone and watch sensor data (accelerometer, gyroscope, GPS)
- **Web dashboard** - Mixpanel-style sidebar UI for managing sessions and viewing processed data
- **Data visualization** - Time-series charts for all data sources with synchronized 3-video playback

## Requirements

- Python 3.10+
- FFmpeg (with VideoToolbox support on macOS)
- Flask

## Installation

```bash
cd syncdrivev1_src
python -m venv .venv
source .venv/bin/activate
pip install flask
```

## Usage

```bash
python syncdrivev1.py
```

Open http://localhost:5050 in your browser.

### Session Structure

```
session_name/
├── melb-01-cam-01/     # Camera folders
│   ├── seg_001.mp4
│   └── ...
├── can_raw.csv         # CAN bus data
├── phone/              # Phone sensors
│   ├── accelerometer.csv
│   ├── gyroscope.csv
│   └── ...
└── watch/              # Watch sensors
    └── ...
```

### Output Structure

```
session_name_processed/
├── melb-01-cam-01_full.mp4   # Concatenated videos
├── parsed/                    # Decoded CAN data
│   ├── wheel_speed.csv
│   ├── vehicle_speed.csv
│   ├── brake.csv
│   ├── steering.csv
│   ├── engine_rpm.csv
│   ├── temperature.csv
│   ├── events.csv
│   └── trip_summary_1hz.csv
├── phone/                     # Copied phone data
├── watch/                     # Copied watch data
├── metadata.json              # Processing info with time ranges
└── processing.log             # Processing log
```

## Configuration

Settings are stored in `config.json`:

- `camera_pattern` - Regex for camera folder names
- `segment_pattern` - Glob pattern for video segments
- `default_rotations` - Cameras requiring 180° rotation
- `timezone` - Timezone for timestamp display
- `use_hardware_accel` - Enable VideoToolbox (macOS)
- `video_quality` - Encoding quality (0-100)

## Web UI Pages

- **Home** - Dashboard overview with processing status
- **Sessions** - Browse and queue sessions for processing
- **Queue** - View processing queue and progress
- **Processed** - View completed sessions with video playback and data charts
- **Guide** - Usage instructions
- **Settings** - Configure processing options
