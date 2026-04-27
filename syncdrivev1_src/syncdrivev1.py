#!/usr/bin/env python3
"""SyncDrive V1 - Process drive recordings"""

import csv
import json
import multiprocessing
import platform
import re
import shutil
import subprocess
import sys
import threading
import time
import queue
import webbrowser
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, render_template, request, jsonify

# Multiprocessing settings
MAX_WORKERS = max(1, multiprocessing.cpu_count() - 1)  # Leave one core free

# Load configuration
CONFIG_PATH = Path(__file__).parent / 'config.json'
DEFAULT_CONFIG = {
    'camera_pattern': r'melb-01-cam-\d+',
    'segment_pattern': 'seg_*.mp4',
    'default_rotations': {'melb-01-cam-01': True, 'melb-01-cam-03': True},
    'timezone': 'Australia/Melbourne',
    'port': 5050,
    'host': '0.0.0.0',
    'cache_ttl_seconds': 5,
    'use_hardware_accel': True,  # Use Apple VideoToolbox on macOS
    'video_quality': 65,  # VideoToolbox quality (0-100, higher=better)
}

def load_config():
    """Load config from file, falling back to defaults."""
    config = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                user_config = json.load(f)
                config.update(user_config)
        except Exception as e:
            print(f"Warning: Could not load config.json: {e}")
    return config

CONFIG = load_config()

app = Flask(__name__)
TIMEZONE = ZoneInfo(CONFIG['timezone'])
BASE_DIR = Path(__file__).parent.parent
SESSIONS_DIR = BASE_DIR / 'sessions'
PROCESSED_DIR = BASE_DIR / 'processed'

# Ensure directories exist
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CAMERA_PATTERN = re.compile(CONFIG['camera_pattern'])

# Queue system
state_lock = threading.Lock()
processing_queue = queue.Queue()
current_session = None
queue_list = []
current_progress = {}
can_status = 'none'
phone_status = 'none'
earpods_status = 'none'
watch_status = 'none'
stop_requested = False

# Session cache
session_cache = []
session_cache_time = 0


def get_can_timestamp_range(can_file, filter_outliers=True):
    """Get first and last timestamp from CAN raw file, filtering outliers."""
    try:
        timestamps = []
        with open(can_file, 'r') as f:
            # Read first 1000 and last 1000 lines to find range efficiently
            lines = f.readlines()
            sample_lines = lines[:1000] + lines[-1000:] if len(lines) > 2000 else lines

            for line in sample_lines:
                parts = line.strip().split(',')
                if len(parts) >= 1:
                    try:
                        ts_ms = int(parts[0])
                        timestamps.append(ts_ms)
                    except:
                        continue

        if not timestamps:
            return None

        if filter_outliers and len(timestamps) > 10:
            # Sort and remove outliers (timestamps far from the median)
            timestamps.sort()
            median_ts = timestamps[len(timestamps) // 2]

            # Filter to timestamps within 1 hour of median
            one_hour_ms = 3600 * 1000
            filtered = [ts for ts in timestamps if abs(ts - median_ts) < one_hour_ms]

            if filtered:
                timestamps = filtered

        first_ts = min(timestamps)
        last_ts = max(timestamps)

        start_dt = datetime.fromtimestamp(first_ts / 1000.0, tz=TIMEZONE)
        end_dt = datetime.fromtimestamp(last_ts / 1000.0, tz=TIMEZONE)
        return {
            'start': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'end': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'start_short': start_dt.strftime('%H:%M:%S'),
            'end_short': end_dt.strftime('%H:%M:%S'),
            'duration_sec': round((last_ts - first_ts) / 1000.0, 1)
        }
    except:
        pass
    return None


def get_sensor_timestamp_range(folder_path):
    """Get timestamp range from phone/watch sensor CSV files."""
    try:
        folder = Path(folder_path)
        if not folder.exists():
            return None

        # Find any CSV file with datetime column
        csv_files = list(folder.glob('*.csv'))
        if not csv_files:
            return None

        first_dt = None
        last_dt = None

        for csv_file in csv_files:
            try:
                with open(csv_file, 'r') as f:
                    reader = csv.reader(f)
                    header = next(reader, None)
                    if not header or 'datetime' not in header:
                        continue

                    dt_idx = header.index('datetime')
                    first_row = next(reader, None)
                    if first_row and len(first_row) > dt_idx:
                        dt_str = first_row[dt_idx]
                        parsed = parse_datetime_str(dt_str)
                        if parsed and (first_dt is None or parsed < first_dt):
                            first_dt = parsed

                    # Read last line
                    f.seek(0)
                    lines = f.readlines()
                    if len(lines) > 1:
                        last_line = lines[-1].strip()
                        if last_line:
                            parts = last_line.split(',')
                            if len(parts) > dt_idx:
                                parsed = parse_datetime_str(parts[dt_idx])
                                if parsed and (last_dt is None or parsed > last_dt):
                                    last_dt = parsed
            except:
                continue

        if first_dt and last_dt:
            # Convert to Melbourne time if needed
            if first_dt.tzinfo is None:
                first_dt = first_dt.replace(tzinfo=TIMEZONE)
            else:
                first_dt = first_dt.astimezone(TIMEZONE)

            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=TIMEZONE)
            else:
                last_dt = last_dt.astimezone(TIMEZONE)

            duration = (last_dt - first_dt).total_seconds()
            return {
                'start': first_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'end': last_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'start_short': first_dt.strftime('%H:%M:%S'),
                'end_short': last_dt.strftime('%H:%M:%S'),
                'duration_sec': round(duration, 1)
            }
    except:
        pass
    return None


def parse_datetime_str(dt_str):
    """Parse various datetime string formats."""
    from datetime import timezone as dt_timezone
    try:
        # Try ISO format with timezone: 2026-04-24T08:46:15.983+10:00
        if 'T' in dt_str and ('+' in dt_str or dt_str.endswith('Z')):
            # Handle +10:00 format
            if '+' in dt_str:
                dt_str = dt_str.replace('+10:00', '+1000').replace('+11:00', '+1100')
            return datetime.fromisoformat(dt_str.replace('Z', '+0000'))
    except:
        pass

    try:
        # Try format: 2026-04-24 08:46:18.825
        return datetime.strptime(dt_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
    except:
        pass

    return None


def get_folder_file_count(folder_path):
    """Count files in a folder recursively."""
    try:
        count = sum(1 for _ in Path(folder_path).rglob('*') if _.is_file())
        return count
    except:
        return 0


def get_single_file_timestamp_range(file_path):
    """Get timestamp range from a single CSV file with datetime column."""
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            return None

        first_dt = None
        last_dt = None

        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header or 'datetime' not in header:
                return None

            dt_idx = header.index('datetime')

            # Get first row
            first_row = next(reader, None)
            if first_row and len(first_row) > dt_idx:
                first_dt = parse_datetime_str(first_row[dt_idx])

            # Read to get last row
            last_row = None
            for row in reader:
                last_row = row
            if last_row and len(last_row) > dt_idx:
                last_dt = parse_datetime_str(last_row[dt_idx])

        if first_dt and last_dt:
            if first_dt.tzinfo is None:
                first_dt = first_dt.replace(tzinfo=TIMEZONE)
            else:
                first_dt = first_dt.astimezone(TIMEZONE)

            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=TIMEZONE)
            else:
                last_dt = last_dt.astimezone(TIMEZONE)

            duration = (last_dt - first_dt).total_seconds()
            return {
                'start': first_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'end': last_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'start_short': first_dt.strftime('%H:%M:%S'),
                'end_short': last_dt.strftime('%H:%M:%S'),
                'duration_sec': round(duration, 1)
            }
    except:
        pass
    return None


def get_processed_folder_name(session_name):
    """Get the processed folder name with _processed suffix."""
    return f"{session_name}_processed"


def calculate_sync_status(can_range, phone_range, watch_range):
    """Calculate sync status between data sources based on time range overlap."""
    ranges = []
    sources = []

    if can_range and 'start' in can_range:
        ranges.append(can_range)
        sources.append('can')
    if phone_range and 'start' in phone_range:
        ranges.append(phone_range)
        sources.append('phone')
    if watch_range and 'start' in watch_range:
        ranges.append(watch_range)
        sources.append('watch')

    if len(ranges) < 2:
        return {
            'synced': False,
            'status': 'insufficient_data',
            'message': 'Need at least 2 data sources to check sync',
            'sources': sources
        }

    # Parse all start/end times
    try:
        starts = []
        ends = []
        for r in ranges:
            start_dt = datetime.strptime(r['start'], '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.strptime(r['end'], '%Y-%m-%d %H:%M:%S')
            starts.append(start_dt)
            ends.append(end_dt)

        # Check for overlap
        latest_start = max(starts)
        earliest_end = min(ends)
        overlap_seconds = (earliest_end - latest_start).total_seconds()

        if overlap_seconds <= 0:
            return {
                'synced': False,
                'status': 'no_overlap',
                'message': 'Data sources do not overlap in time',
                'sources': sources
            }

        # Calculate how much of each source is in the overlap
        total_duration = max((e - s).total_seconds() for s, e in zip(starts, ends))
        overlap_pct = (overlap_seconds / total_duration) * 100 if total_duration > 0 else 0

        # Check for outliers (like the CAN garbage timestamp issue)
        max_gap = max((latest_start - s).total_seconds() for s in starts)
        has_outlier = max_gap > 60  # More than 1 minute gap suggests bad timestamps

        return {
            'synced': overlap_pct > 90 and not has_outlier,
            'status': 'synced' if (overlap_pct > 90 and not has_outlier) else 'partial',
            'overlap_seconds': round(overlap_seconds, 1),
            'overlap_pct': round(overlap_pct, 1),
            'has_outlier': has_outlier,
            'common_start': latest_start.strftime('%H:%M:%S'),
            'common_end': earliest_end.strftime('%H:%M:%S'),
            'sources': sources,
            'message': 'Data sources are synchronized' if overlap_pct > 90 else f'Partial overlap ({overlap_pct:.0f}%)'
        }
    except Exception as e:
        return {
            'synced': False,
            'status': 'error',
            'message': f'Error calculating sync: {str(e)}',
            'sources': sources
        }


def find_processed_folder(session_name):
    """Find the processed folder (checks both old and new naming)."""
    # Check new naming first
    new_path = PROCESSED_DIR / get_processed_folder_name(session_name)
    if new_path.exists():
        return new_path

    # Check old naming for backwards compatibility
    old_path = PROCESSED_DIR / session_name
    if old_path.exists():
        return old_path

    return None


def get_session_metadata(session_path):
    """Get session info from manifest.json and folder structure."""
    base = Path(session_path)

    # Source data status
    source_can = base / 'can_raw.csv'
    source_phone = base / 'phone'
    source_watch = base / 'watch'
    source_earpods = base / 'phone' / 'headphonemotion.csv'

    # Get time ranges for each data source
    can_range = get_can_timestamp_range(source_can) if source_can.exists() else None
    phone_range = get_sensor_timestamp_range(source_phone) if source_phone.exists() else None
    watch_range = get_sensor_timestamp_range(source_watch) if source_watch.exists() else None
    earpods_range = get_single_file_timestamp_range(source_earpods) if source_earpods.exists() else None

    # Calculate sync status - check if time ranges overlap
    sync_status = calculate_sync_status(can_range, phone_range, watch_range)

    info = {
        'name': base.name,
        'path': str(base),
        'cameras': 0,
        'segments': 0,
        'duration_sec': 0,
        'size_bytes': 0,
        'start_time': None,
        'end_time': None,
        # Source data availability
        'source': {
            'can': source_can.exists(),
            'phone': source_phone.exists(),
            'watch': source_watch.exists(),
            'earpods': source_earpods.exists(),
            'phone_files': get_folder_file_count(source_phone) if source_phone.exists() else 0,
            'watch_files': get_folder_file_count(source_watch) if source_watch.exists() else 0,
            'can_time_range': can_range,
            'phone_time_range': phone_range,
            'watch_time_range': watch_range,
            'earpods_time_range': earpods_range,
            'sync_status': sync_status,
        },
        # Legacy fields for compatibility
        'has_can': source_can.exists(),
        'has_phone': source_phone.exists(),
        'has_watch': source_watch.exists(),
        'has_earpods': source_earpods.exists(),
    }

    manifest_path = base / 'manifest.json'
    if manifest_path.exists():
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
                info['start_time'] = manifest.get('start_time')
                info['end_time'] = manifest.get('end_time')
        except:
            pass

    camera_details = []
    for item in sorted(base.iterdir(), key=lambda x: x.name):
        if item.is_dir() and CAMERA_PATTERN.match(item.name):
            info['cameras'] += 1
            segs = list(item.glob(CONFIG['segment_pattern']))
            seg_count = len(segs)
            info['segments'] += seg_count
            camera_details.append({'name': item.name, 'segments': seg_count})
            for seg in segs:
                info['size_bytes'] += seg.stat().st_size
    info['camera_details'] = camera_details

    # Check for processed folder (supports both old and new naming)
    processed_path = find_processed_folder(base.name)
    info['is_processed'] = processed_path is not None

    if info['is_processed']:
        info['output_path'] = str(processed_path)

        # Check what outputs exist in processed folder
        proc_can_dir = processed_path / 'parsed'
        proc_phone = processed_path / 'phone'
        proc_watch = processed_path / 'watch'

        # Count processed videos
        proc_videos = list(processed_path.glob('*_full.mp4'))

        # Read metadata.json if it exists
        proc_metadata = {}
        metadata_file = processed_path / 'metadata.json'
        if metadata_file.exists():
            try:
                with open(metadata_file) as f:
                    proc_metadata = json.load(f)
            except:
                pass

        # Check for processing log and earpods
        proc_log = processed_path / 'processing.log'
        proc_earpods = processed_path / 'phone' / 'headphonemotion.csv'

        info['processed'] = {
            'videos': len(proc_videos),
            'video_names': [v.stem.replace('_full', '') for v in proc_videos],
            'can': proc_can_dir.exists(),
            'can_files': list(f.name for f in proc_can_dir.glob('*.csv')) if proc_can_dir.exists() else [],
            'phone': proc_phone.exists(),
            'watch': proc_watch.exists(),
            'earpods': proc_earpods.exists(),
            'phone_files': get_folder_file_count(proc_phone) if proc_phone.exists() else 0,
            'watch_files': get_folder_file_count(proc_watch) if proc_watch.exists() else 0,
            'metadata': proc_metadata,
            'has_log': proc_log.exists(),
        }

    return info


def get_all_sessions():
    """Get all sessions with metadata (cached)."""
    global session_cache, session_cache_time

    now = time.time()
    cache_ttl = CONFIG.get('cache_ttl_seconds', 5)

    if now - session_cache_time < cache_ttl and session_cache:
        return session_cache

    sessions = []
    if not SESSIONS_DIR.exists():
        return sessions

    for item in sorted(SESSIONS_DIR.iterdir()):
        if not item.is_dir() or item.name.startswith('.'):
            continue

        has_cameras = any(CAMERA_PATTERN.match(d.name) for d in item.iterdir() if d.is_dir())
        has_can = (item / 'can_raw.csv').exists()

        if has_cameras or has_can:
            sessions.append(get_session_metadata(item))

    session_cache = sessions
    session_cache_time = now
    return sessions


def invalidate_session_cache():
    """Clear the session cache to force refresh."""
    global session_cache_time
    session_cache_time = 0


def get_segment_duration(filepath):
    try:
        result = subprocess.run([
            'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1', filepath
        ], capture_output=True, text=True, timeout=10)
        return float(result.stdout.strip())
    except:
        return None


def open_folder_cross_platform(path):
    """Open a folder in the system file manager (cross-platform)."""
    path = Path(path)
    if not path.exists():
        return False

    system = platform.system()
    try:
        if system == 'Darwin':
            subprocess.run(['open', str(path)])
        elif system == 'Windows':
            subprocess.run(['explorer', str(path)])
        elif system == 'Linux':
            subprocess.run(['xdg-open', str(path)])
        else:
            webbrowser.open(f'file://{path}')
        return True
    except Exception as e:
        print(f"Error opening folder: {e}")
        return False


def get_sync_time_range(base_path):
    """Get the time range from phone/watch data for syncing CAN data."""
    base = Path(base_path)
    min_ts = None
    max_ts = None

    for folder in ['phone', 'watch']:
        folder_path = base / folder
        if not folder_path.exists():
            continue

        time_range = get_sensor_timestamp_range(folder_path)
        if time_range:
            # Parse the datetime strings back to timestamps
            try:
                start_dt = datetime.strptime(time_range['start'], '%Y-%m-%d %H:%M:%S')
                end_dt = datetime.strptime(time_range['end'], '%Y-%m-%d %H:%M:%S')
                start_dt = start_dt.replace(tzinfo=TIMEZONE)
                end_dt = end_dt.replace(tzinfo=TIMEZONE)

                start_ms = int(start_dt.timestamp() * 1000)
                end_ms = int(end_dt.timestamp() * 1000)

                if min_ts is None or start_ms < min_ts:
                    min_ts = start_ms
                if max_ts is None or end_ms > max_ts:
                    max_ts = end_ms
            except:
                continue

    if min_ts and max_ts:
        # Add 5 second buffer on each side
        return (min_ts - 5000, max_ts + 5000)
    return None


def process_can_data(base_path, output_dir):
    global can_status
    raw_file = Path(base_path) / 'can_raw.csv'
    if not raw_file.exists():
        return 0

    with state_lock:
        can_status = 'processing'

    # Get sync time range from phone/watch data
    sync_range = get_sync_time_range(base_path)
    if sync_range:
        print(f"Syncing CAN data to phone/watch range: {sync_range[0]} - {sync_range[1]}", flush=True)

    data_by_id = defaultdict(list)
    start_time = None
    total_lines = 0
    filtered_lines = 0

    with open(raw_file, 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) < 4:
                continue
            try:
                ts_ms, can_id, data = int(parts[0]), parts[1], parts[3]
            except:
                continue

            total_lines += 1

            # Filter to sync range if available
            if sync_range:
                if ts_ms < sync_range[0] or ts_ms > sync_range[1]:
                    continue
                filtered_lines += 1

            if start_time is None:
                start_time = ts_ms
            rel_time = (ts_ms - start_time) / 1000.0
            dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=TIMEZONE)
            data_by_id[can_id].append({
                'time': rel_time,
                'timestamp_ms': ts_ms,
                'melbourne_time': dt.strftime('%H:%M:%S.%f')[:-3],
                'data': data
            })

    if sync_range:
        print(f"CAN data: kept {filtered_lines}/{total_lines} lines ({100*filtered_lines/total_lines:.1f}%)", flush=True)

    can_dir = Path(output_dir) / 'parsed' / 'can'
    can_dir.mkdir(parents=True, exist_ok=True)
    for can_id, rows in data_by_id.items():
        with open(can_dir / f'{can_id}.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['time', 'melbourne_time', 'data'])
            for row in rows:
                writer.writerow([row['time'], row['melbourne_time'], row['data']])

    # Generate human-readable parsed files
    parsed_dir = Path(output_dir) / 'parsed'
    parsed_dir.mkdir(parents=True, exist_ok=True)

    parse_human_readable_can(data_by_id, parsed_dir)

    with state_lock:
        can_status = 'complete'
    return len(data_by_id)


def parse_human_readable_can(data_by_id, parsed_dir):
    """Parse CAN data into human-readable CSV files."""

    # Parse wheel speed from 0B0 and 0B2
    parse_wheel_speed(data_by_id, parsed_dir)

    # Parse vehicle speed from 610 and 0B4
    parse_vehicle_speed(data_by_id, parsed_dir)

    # Parse brake status from 224
    parse_brake(data_by_id, parsed_dir)

    # Parse steering angle from 260
    parse_steering(data_by_id, parsed_dir)

    # Parse engine RPM from 2C4
    parse_engine_rpm(data_by_id, parsed_dir)

    # Parse temperature from 3A0
    parse_temperature(data_by_id, parsed_dir)

    # Create combined events file
    create_combined_events(data_by_id, parsed_dir)

    # Create 1Hz trip summary
    create_trip_summary(data_by_id, parsed_dir)


def parse_wheel_speed(data_by_id, parsed_dir):
    """Parse 0B0 and 0B2 wheel speed data."""
    rows = []

    # Process 0B0 (wheels 1 & 2)
    for row in data_by_id.get('0B0', []):
        try:
            data = row['data'].zfill(12)
            speed1 = int(data[0:4], 16) * 0.01
            speed2 = int(data[4:8], 16) * 0.01
            rows.append({
                'time': row['time'],
                'melbourne_time': row['melbourne_time'],
                'wheel1_kmh': round(speed1, 2),
                'wheel2_kmh': round(speed2, 2),
                'source': '0B0'
            })
        except (ValueError, KeyError):
            continue

    # Process 0B2 (wheels 3 & 4)
    for row in data_by_id.get('0B2', []):
        try:
            data = row['data'].zfill(12)
            speed3 = int(data[0:4], 16) * 0.01
            speed4 = int(data[4:8], 16) * 0.01
            rows.append({
                'time': row['time'],
                'melbourne_time': row['melbourne_time'],
                'wheel3_kmh': round(speed3, 2),
                'wheel4_kmh': round(speed4, 2),
                'source': '0B2'
            })
        except (ValueError, KeyError):
            continue

    if not rows:
        return

    rows.sort(key=lambda x: x['time'])

    # Merge nearby timestamps
    merged = defaultdict(dict)
    for row in rows:
        t = round(row['time'], 2)
        merged[t]['melbourne_time'] = row['melbourne_time']
        if row['source'] == '0B0':
            merged[t]['wheel1'] = row['wheel1_kmh']
            merged[t]['wheel2'] = row['wheel2_kmh']
        else:
            merged[t]['wheel3'] = row['wheel3_kmh']
            merged[t]['wheel4'] = row['wheel4_kmh']

    with open(parsed_dir / 'wheel_speed.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time_sec', 'melbourne_time', 'wheel1_kmh', 'wheel2_kmh', 'wheel3_kmh', 'wheel4_kmh', 'avg_speed_kmh'])
        for t in sorted(merged.keys()):
            d = merged[t]
            w1 = d.get('wheel1', '')
            w2 = d.get('wheel2', '')
            w3 = d.get('wheel3', '')
            w4 = d.get('wheel4', '')
            speeds = [s for s in [w1, w2, w3, w4] if s != '']
            avg = round(sum(speeds) / len(speeds), 2) if speeds else ''
            writer.writerow([t, d.get('melbourne_time', ''), w1, w2, w3, w4, avg])


def parse_vehicle_speed(data_by_id, parsed_dir):
    """Parse 610 vehicle speed and 0B4 speed data."""
    rows = []

    # Process 610 (dashboard speed)
    for row in data_by_id.get('610', []):
        try:
            data = row['data'].zfill(16)
            speed = int(data[4:6], 16)
            rows.append({
                'time': row['time'],
                'melbourne_time': row['melbourne_time'],
                'speed_kmh': speed,
                'source': '610'
            })
        except (ValueError, KeyError):
            continue

    # Process 0B4 (vehicle speed)
    for row in data_by_id.get('0B4', []):
        try:
            data = row['data'].zfill(16)
            raw = int(data[8:12], 16)
            speed = raw / 100.0
            rows.append({
                'time': row['time'],
                'melbourne_time': row['melbourne_time'],
                'speed_kmh': round(speed, 2),
                'source': '0B4'
            })
        except (ValueError, KeyError):
            continue

    if not rows:
        return

    rows.sort(key=lambda x: x['time'])

    with open(parsed_dir / 'vehicle_speed.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time_sec', 'melbourne_time', 'speed_kmh', 'source'])
        for row in rows:
            writer.writerow([row['time'], row['melbourne_time'], row['speed_kmh'], row['source']])


def parse_brake(data_by_id, parsed_dir):
    """Parse 224 brake pedal status."""
    rows = []

    for row in data_by_id.get('224', []):
        try:
            data = row['data'].zfill(16)
            byte1 = int(data[0:2], 16)
            brake_on = (byte1 & 0x20) != 0
            rows.append({
                'time': row['time'],
                'melbourne_time': row['melbourne_time'],
                'brake_pressed': 'YES' if brake_on else 'NO',
                'raw_hex': hex(byte1)
            })
        except (ValueError, KeyError):
            continue

    if not rows:
        return

    with open(parsed_dir / 'brake.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time_sec', 'melbourne_time', 'brake_pressed', 'raw_hex'])
        for row in rows:
            writer.writerow([row['time'], row['melbourne_time'], row['brake_pressed'], row['raw_hex']])


def parse_steering(data_by_id, parsed_dir):
    """Parse 260 steering angle with multiple interpretations."""
    rows = []

    for row in data_by_id.get('260', []):
        try:
            data = row['data'].zfill(16)
            if 'E' in data or '+' in data:
                continue
            raw = int(data[12:16], 16)
            if raw > 0x7FFF:
                raw -= 0x10000

            wheel_angle_div10 = raw / 10.0
            wheel_angle_div100 = raw / 100.0
            road_wheel_div1000 = raw / 1000.0

            if abs(wheel_angle_div100) > 900:
                continue

            if raw > 50:
                direction = 'LEFT'
            elif raw < -50:
                direction = 'RIGHT'
            else:
                direction = 'CENTER'

            rows.append({
                'time': row['time'],
                'melbourne_time': row['melbourne_time'],
                'raw_value': raw,
                'steering_wheel_deg_div10': round(wheel_angle_div10, 1),
                'steering_wheel_deg_div100': round(wheel_angle_div100, 2),
                'road_wheel_deg_div1000': round(road_wheel_div1000, 3),
                'direction': direction
            })
        except (ValueError, KeyError):
            continue

    if not rows:
        return

    with open(parsed_dir / 'steering.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time_sec', 'melbourne_time', 'raw_value', 'steering_wheel_deg_div10',
                        'steering_wheel_deg_div100', 'road_wheel_deg_div1000', 'direction'])
        for row in rows:
            writer.writerow([row['time'], row['melbourne_time'], row['raw_value'],
                           row['steering_wheel_deg_div10'], row['steering_wheel_deg_div100'],
                           row['road_wheel_deg_div1000'], row['direction']])


def parse_engine_rpm(data_by_id, parsed_dir):
    """Parse 2C4 engine RPM."""
    rows = []

    for row in data_by_id.get('2C4', []):
        try:
            data = row['data'].zfill(16)
            raw = int(data[0:4], 16)
            rpm = raw
            rows.append({
                'time': row['time'],
                'melbourne_time': row['melbourne_time'],
                'engine_rpm': rpm,
                'engine_state': 'RUNNING' if rpm > 500 else 'IDLE/OFF'
            })
        except (ValueError, KeyError):
            continue

    if not rows:
        return

    with open(parsed_dir / 'engine_rpm.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time_sec', 'melbourne_time', 'engine_rpm', 'engine_state'])
        for row in rows:
            writer.writerow([row['time'], row['melbourne_time'], row['engine_rpm'], row['engine_state']])


def parse_temperature(data_by_id, parsed_dir):
    """Parse 3A0 temperature."""
    rows = []

    for row in data_by_id.get('3A0', []):
        try:
            data = row['data'].zfill(16)
            raw = int(data[14:16], 16)
            temp_c = raw - 40
            rows.append({
                'time': row['time'],
                'melbourne_time': row['melbourne_time'],
                'temperature_c': temp_c,
                'raw_value': raw
            })
        except (ValueError, KeyError):
            continue

    if not rows:
        return

    with open(parsed_dir / 'temperature.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time_sec', 'melbourne_time', 'temperature_c', 'raw_value'])
        for row in rows:
            writer.writerow([row['time'], row['melbourne_time'], row['temperature_c'], row['raw_value']])


def create_combined_events(data_by_id, parsed_dir):
    """Create a combined events file with significant moments."""
    events = []

    # Detect stops and speed peaks from 0B0
    prev_speed = 0
    for row in data_by_id.get('0B0', []):
        try:
            data = row['data'].zfill(12)
            speed = int(data[0:4], 16) * 0.01

            if speed < 0.5 and prev_speed >= 0.5:
                events.append({
                    'time': row['time'],
                    'melbourne_time': row['melbourne_time'],
                    'event': 'STOP',
                    'value': f'{prev_speed:.1f} -> 0 km/h',
                    'details': 'Vehicle came to stop'
                })

            if speed > 40 and prev_speed < speed:
                events.append({
                    'time': row['time'],
                    'melbourne_time': row['melbourne_time'],
                    'event': 'SPEED_PEAK',
                    'value': f'{speed:.1f} km/h',
                    'details': 'High speed'
                })

            prev_speed = speed
        except (ValueError, KeyError):
            continue

    # Detect brake events from 224
    prev_brake = False
    for row in data_by_id.get('224', []):
        try:
            data = row['data'].zfill(16)
            byte1 = int(data[0:2], 16)
            brake_on = (byte1 & 0x20) != 0

            if brake_on and not prev_brake:
                events.append({
                    'time': row['time'],
                    'melbourne_time': row['melbourne_time'],
                    'event': 'BRAKE_ON',
                    'value': 'Pressed',
                    'details': 'Brake pedal pressed'
                })
            elif not brake_on and prev_brake:
                events.append({
                    'time': row['time'],
                    'melbourne_time': row['melbourne_time'],
                    'event': 'BRAKE_OFF',
                    'value': 'Released',
                    'details': 'Brake pedal released'
                })

            prev_brake = brake_on
        except (ValueError, KeyError):
            continue

    if not events:
        return

    events.sort(key=lambda x: x['time'])

    with open(parsed_dir / 'events.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time_sec', 'melbourne_time', 'event_type', 'value', 'details'])
        for e in events:
            writer.writerow([e['time'], e['melbourne_time'], e['event'], e['value'], e['details']])


def create_trip_summary(data_by_id, parsed_dir):
    """Create a 1Hz summary of all signals for easy analysis."""
    data_by_second = defaultdict(lambda: {
        'wheel_speed': None,
        'vehicle_speed': None,
        'brake': None,
        'steering': None,
        'rpm': None,
        'temp': None,
        'melbourne_time': None
    })

    # Wheel speed from 0B0
    for row in data_by_id.get('0B0', []):
        try:
            t = int(row['time'])
            data = row['data'].zfill(12)
            speed = int(data[0:4], 16) * 0.01
            data_by_second[t]['wheel_speed'] = round(speed, 1)
            data_by_second[t]['melbourne_time'] = row['melbourne_time'][:8]
        except (ValueError, KeyError):
            continue

    # Vehicle speed from 610
    for row in data_by_id.get('610', []):
        try:
            t = int(row['time'])
            data = row['data'].zfill(16)
            speed = int(data[4:6], 16)
            data_by_second[t]['vehicle_speed'] = speed
        except (ValueError, KeyError):
            continue

    # Brake from 224
    for row in data_by_id.get('224', []):
        try:
            t = int(row['time'])
            data = row['data'].zfill(16)
            byte1 = int(data[0:2], 16)
            brake_on = (byte1 & 0x20) != 0
            data_by_second[t]['brake'] = 'ON' if brake_on else 'OFF'
        except (ValueError, KeyError):
            continue

    # Steering from 260
    for row in data_by_id.get('260', []):
        try:
            t = int(row['time'])
            data = row['data'].zfill(16)
            if 'E' in data or '+' in data:
                continue
            raw = int(data[12:16], 16)
            if raw > 0x7FFF:
                raw -= 0x10000
            if abs(raw) <= 90000:
                data_by_second[t]['steering'] = raw
        except (ValueError, KeyError):
            continue

    # RPM from 2C4
    for row in data_by_id.get('2C4', []):
        try:
            t = int(row['time'])
            data = row['data'].zfill(16)
            rpm = int(data[0:4], 16)
            data_by_second[t]['rpm'] = rpm
        except (ValueError, KeyError):
            continue

    # Temperature from 3A0
    for row in data_by_id.get('3A0', []):
        try:
            t = int(row['time'])
            data = row['data'].zfill(16)
            raw = int(data[14:16], 16)
            temp = raw - 40
            data_by_second[t]['temp'] = temp
        except (ValueError, KeyError):
            continue

    if not data_by_second:
        return

    with open(parsed_dir / 'trip_summary_1hz.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time_sec', 'melbourne_time', 'wheel_speed_kmh', 'vehicle_speed_kmh',
                        'brake', 'steering_raw', 'engine_rpm', 'temperature_c'])
        for t in sorted(data_by_second.keys()):
            d = data_by_second[t]
            writer.writerow([
                t,
                d['melbourne_time'] or '',
                d['wheel_speed'] if d['wheel_speed'] is not None else '',
                d['vehicle_speed'] if d['vehicle_speed'] is not None else '',
                d['brake'] or '',
                d['steering'] if d['steering'] is not None else '',
                d['rpm'] if d['rpm'] is not None else '',
                d['temp'] if d['temp'] is not None else ''
            ])


def process_camera(camera_name, camera_path, segments, needs_rotation, output_dir):
    global current_progress

    with state_lock:
        current_progress[camera_name] = {'status': 'processing', 'progress': 0, 'skipped': 0}

    try:
        if not segments:
            with state_lock:
                current_progress[camera_name] = {'status': 'complete', 'progress': 100, 'skipped': 0}
            return

        total_duration = 0
        valid_segments = []
        skipped_count = 0

        for seg_path in segments:
            dur = get_segment_duration(seg_path)
            if dur:
                valid_segments.append(seg_path)
                total_duration += dur
            else:
                skipped_count += 1
                print(f"WARNING: Skipping corrupted segment: {seg_path}", flush=True)

        with state_lock:
            current_progress[camera_name]['skipped'] = skipped_count

        if not valid_segments:
            with state_lock:
                current_progress[camera_name] = {'status': 'complete', 'progress': 100, 'skipped': skipped_count}
            return

        list_path = Path(camera_path) / f'.concat_list.txt'
        with open(list_path, 'w') as f:
            for seg in valid_segments:
                f.write(f"file '{seg}'\n")

        output_path = Path(output_dir) / f"{camera_name}_full.mp4"

        # Build FFmpeg command with optional hardware acceleration
        use_hw = CONFIG.get('use_hardware_accel', True) and platform.system() == 'Darwin'

        cmd = ['ffmpeg', '-y']

        # Hardware-accelerated decoding on macOS
        if use_hw:
            cmd.extend(['-hwaccel', 'videotoolbox'])

        cmd.extend(['-f', 'concat', '-safe', '0', '-i', str(list_path)])

        # Video filters (rotation if needed)
        if needs_rotation:
            cmd.extend(['-vf', 'hflip,vflip'])

        # Encoding settings
        if use_hw:
            # Apple VideoToolbox hardware encoding
            quality = CONFIG.get('video_quality', 65)
            cmd.extend([
                '-c:v', 'h264_videotoolbox',
                '-q:v', str(quality),  # Quality (0-100)
                '-allow_sw', '1',  # Allow software fallback
            ])
            print(f"Using VideoToolbox hardware acceleration for {camera_name}", flush=True)
        else:
            # Software encoding fallback
            cmd.extend([
                '-c:v', 'libx264',
                '-preset', 'medium',
                '-crf', '20',
            ])

        cmd.extend(['-c:a', 'aac', '-movflags', '+faststart', str(output_path)])

        process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)

        for line in process.stderr:
            match = re.search(r'time=(\d+):(\d+):(\d+\.?\d*)', line)
            if match:
                h, m, s = match.groups()
                current = int(h)*3600 + int(m)*60 + float(s)
                progress = min(100, int((current / total_duration) * 100)) if total_duration > 0 else 0
                with state_lock:
                    current_progress[camera_name]['progress'] = progress

        process.wait()
        list_path.unlink(missing_ok=True)

        with state_lock:
            if process.returncode == 0:
                current_progress[camera_name] = {'status': 'complete', 'progress': 100, 'skipped': skipped_count}
            else:
                current_progress[camera_name] = {'status': 'error', 'progress': 0, 'skipped': skipped_count}
                print(f"ERROR: FFmpeg failed for {camera_name}", flush=True)
    except Exception as e:
        print(f"ERROR processing {camera_name}: {e}", flush=True)
        with state_lock:
            current_progress[camera_name] = {'status': 'error', 'progress': 0, 'skipped': 0}


class ProcessingLogger:
    """Logger that writes to both stdout and a log file."""
    def __init__(self, log_path):
        self.log_path = Path(log_path)
        self.log_file = open(self.log_path, 'w')
        self.start_time = datetime.now(tz=TIMEZONE)
        self.log(f"Processing started at {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    def log(self, message):
        timestamp = datetime.now(tz=TIMEZONE).strftime('%H:%M:%S')
        line = f"[{timestamp}] {message}"
        print(line, flush=True)
        self.log_file.write(line + '\n')
        self.log_file.flush()

    def close(self):
        duration = (datetime.now(tz=TIMEZONE) - self.start_time).total_seconds()
        self.log(f"Processing completed in {duration:.1f} seconds")
        self.log_file.close()


def process_session(session_path, session_name, rotations):
    """Process a single session using parallel processing."""
    global current_progress, can_status, phone_status, earpods_status, watch_status

    # Use new naming convention with _processed suffix
    output_dir = PROCESSED_DIR / get_processed_folder_name(session_name)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize logger
    logger = ProcessingLogger(output_dir / 'processing.log')

    base = Path(session_path)
    has_can = (base / 'can_raw.csv').exists()
    has_phone = (base / 'phone').exists()
    has_watch = (base / 'watch').exists()

    has_earpods = (base / 'phone' / 'headphonemotion.csv').exists()

    with state_lock:
        current_progress = {}
        can_status = 'pending' if has_can else 'none'
        phone_status = 'pending' if has_phone else 'none'
        earpods_status = 'pending' if has_earpods else 'none'
        watch_status = 'pending' if has_watch else 'none'

    # STEP 1: Copy phone/watch FIRST (needed for CAN time range filtering)
    logger.log("Step 1: Copying sensor data (phone/watch)")

    # Copy phone folder
    if has_phone:
        with state_lock:
            phone_status = 'processing'
            earpods_status = 'processing' if has_earpods else 'none'
        src = base / 'phone'
        dst = output_dir / 'phone'
        try:
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            file_count = sum(1 for _ in dst.rglob('*') if _.is_file())
            logger.log(f"  Copied phone: {file_count} files")
            with state_lock:
                phone_status = 'complete'
                earpods_status = 'complete' if has_earpods else 'none'
        except Exception as e:
            logger.log(f"  ERROR copying phone: {e}")
            with state_lock:
                phone_status = 'error'
                earpods_status = 'error' if has_earpods else 'none'
    else:
        logger.log("  phone: not found in source")

    # Copy watch folder
    if has_watch:
        with state_lock:
            watch_status = 'processing'
        src = base / 'watch'
        dst = output_dir / 'watch'
        try:
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            file_count = sum(1 for _ in dst.rglob('*') if _.is_file())
            logger.log(f"  Copied watch: {file_count} files")
            with state_lock:
                watch_status = 'complete'
        except Exception as e:
            logger.log(f"  ERROR copying watch: {e}")
            with state_lock:
                watch_status = 'error'
    else:
        logger.log("  watch: not found in source")

    # STEP 2: Process CAN data (filtered to phone/watch time range)
    if has_can:
        logger.log("Step 2: Processing CAN data")
        try:
            can_ids = process_can_data(session_path, str(output_dir))
            logger.log(f"  Parsed {can_ids} CAN IDs with human-readable decoding")
        except Exception as e:
            logger.log(f"  ERROR processing CAN: {e}")
    else:
        logger.log("Step 2: CAN data not found, skipping")

    # STEP 3: Find and prepare cameras
    cameras = []
    for item in sorted(base.iterdir()):
        if item.is_dir() and CAMERA_PATTERN.match(item.name):
            segments = sorted([str(s) for s in item.glob(CONFIG['segment_pattern'])])
            needs_rotation = rotations.get(item.name, CONFIG['default_rotations'].get(item.name, False))
            cameras.append((item.name, str(item), segments, needs_rotation))
            with state_lock:
                current_progress[item.name] = {'status': 'pending', 'progress': 0}

    logger.log(f"Step 3: Processing {len(cameras)} cameras (VideoToolbox: {CONFIG.get('use_hardware_accel', True)})")

    # STEP 4: Process videos in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for cam_name, cam_path, segments, needs_rotation in cameras:
            logger.log(f"  {cam_name}: {len(segments)} segments" + (" (rotation)" if needs_rotation else ""))
            future = executor.submit(process_camera, cam_name, cam_path, segments, needs_rotation, str(output_dir))
            futures[future] = cam_name

        for future in futures:
            cam_name = futures[future]
            try:
                future.result()
                with state_lock:
                    status = current_progress.get(cam_name, {})
                skipped = status.get('skipped', 0)
                if skipped > 0:
                    logger.log(f"  {cam_name}: complete ({skipped} corrupted segments skipped)")
                else:
                    logger.log(f"  {cam_name}: complete")
            except Exception as e:
                logger.log(f"  {cam_name}: ERROR - {e}")

    # STEP 5: Create metadata
    logger.log("Step 4: Creating metadata.json")
    create_processing_metadata(base, output_dir, session_name)

    logger.close()
    invalidate_session_cache()


def create_processing_metadata(source_path, output_path, session_name):
    """Create metadata.json with timestamp ranges for all data sources."""
    metadata = {
        'session_name': session_name,
        'source_path': str(source_path),
        'processed_at': datetime.now(tz=TIMEZONE).strftime('%Y-%m-%d %H:%M:%S'),
        'timezone': str(TIMEZONE),
        'data_sources': {}
    }

    # CAN data timestamp range
    can_file = source_path / 'can_raw.csv'
    if can_file.exists():
        can_range = get_can_timestamp_range(can_file)
        if can_range:
            metadata['data_sources']['can'] = can_range

    # Phone data timestamp range
    phone_dir = output_path / 'phone'
    if phone_dir.exists():
        phone_range = get_sensor_timestamp_range(phone_dir)
        if phone_range:
            metadata['data_sources']['phone'] = phone_range

    # Watch data timestamp range
    watch_dir = output_path / 'watch'
    if watch_dir.exists():
        watch_range = get_sensor_timestamp_range(watch_dir)
        if watch_range:
            metadata['data_sources']['watch'] = watch_range

    # Video files info
    videos = list(output_path.glob('*_full.mp4'))
    if videos:
        video_info = []
        for video in videos:
            try:
                duration = get_segment_duration(str(video))
                video_info.append({
                    'name': video.name,
                    'duration_sec': round(duration, 1) if duration else None
                })
            except:
                video_info.append({'name': video.name, 'duration_sec': None})
        metadata['data_sources']['videos'] = video_info

    # Calculate overall time range across all sources
    all_starts = []
    all_ends = []
    for source, data in metadata['data_sources'].items():
        if source == 'videos':
            continue
        if isinstance(data, dict) and 'start' in data:
            all_starts.append(data['start'])
            all_ends.append(data['end'])

    if all_starts and all_ends:
        metadata['overall_time_range'] = {
            'start': min(all_starts),
            'end': max(all_ends)
        }

    # Write metadata.json
    metadata_file = output_path / 'metadata.json'
    try:
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"Created metadata.json in {output_path}", flush=True)
    except Exception as e:
        print(f"Error creating metadata.json: {e}", flush=True)


def queue_worker():
    """Background worker that processes queued sessions."""
    global current_session, current_progress, can_status, phone_status, earpods_status, watch_status, stop_requested

    while True:
        item = processing_queue.get()
        if item is None:
            break

        with state_lock:
            if stop_requested:
                stop_requested = False
                processing_queue.task_done()
                continue

        session_path, session_name, rotations = item

        with state_lock:
            current_session = session_name
            if session_name in queue_list:
                queue_list.remove(session_name)

        try:
            process_session(session_path, session_name, rotations)
        except Exception as e:
            print(f"Error processing {session_name}: {e}")

        with state_lock:
            current_session = None
            current_progress = {}
            can_status = 'none'
            phone_status = 'none'
            earpods_status = 'none'
            watch_status = 'none'

        processing_queue.task_done()


# Start background worker
worker_thread = threading.Thread(target=queue_worker, daemon=True)
worker_thread.start()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/sessions')
def get_sessions():
    return jsonify(get_all_sessions())


@app.route('/status')
def get_status():
    with state_lock:
        return jsonify({
            'current': current_session,
            'queue': queue_list.copy(),
            'progress': dict(current_progress),
            'can_status': can_status,
            'phone_status': phone_status,
            'earpods_status': earpods_status,
            'watch_status': watch_status
        })


@app.route('/queue', methods=['POST'])
def add_to_queue():
    data = request.json
    path = data.get('path', '')
    name = data.get('name', '')
    rotations = data.get('rotations', {})

    with state_lock:
        if name not in queue_list and name != current_session:
            queue_list.append(name)
            processing_queue.put((path, name, rotations))

    return jsonify({'status': 'ok'})


@app.route('/remove-from-queue', methods=['POST'])
def remove_from_queue():
    name = request.json.get('name', '')
    with state_lock:
        if name in queue_list:
            queue_list.remove(name)
    return jsonify({'status': 'ok'})


@app.route('/open-folder', methods=['POST'])
def open_folder():
    path = request.json.get('path', '')
    if path:
        open_folder_cross_platform(path)
    return jsonify({'status': 'ok'})


@app.route('/reprocess', methods=['POST'])
def reprocess():
    name = request.json.get('name', '')
    # Check both old and new naming conventions
    processed_path = find_processed_folder(name)
    if processed_path and processed_path.exists():
        shutil.rmtree(processed_path)
    invalidate_session_cache()
    return jsonify({'status': 'ok'})


@app.route('/log/<session_name>')
def get_processing_log(session_name):
    """Get the processing log for a session."""
    processed_path = find_processed_folder(session_name)
    if not processed_path:
        return jsonify({'error': 'Session not found'}), 404

    log_path = processed_path / 'processing.log'
    if not log_path.exists():
        return jsonify({'error': 'No processing log found'}), 404

    try:
        with open(log_path, 'r') as f:
            content = f.read()
        return jsonify({'log': content})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/stop', methods=['POST'])
def stop_queue():
    global stop_requested, current_session, queue_list, current_progress, can_status, phone_status, earpods_status, watch_status
    with state_lock:
        stop_requested = True
        current_session = None
        queue_list = []
        current_progress = {}
        can_status = 'none'
        phone_status = 'none'
        earpods_status = 'none'
        watch_status = 'none'
    return jsonify({'status': 'ok'})


@app.route('/config', methods=['GET'])
def get_config():
    """Return current configuration."""
    return jsonify(CONFIG)


@app.route('/config', methods=['POST'])
def save_config():
    """Save configuration to file."""
    global CONFIG, TIMEZONE, CAMERA_PATTERN

    try:
        new_config = request.json

        # Validate required fields
        required = ['camera_pattern', 'segment_pattern', 'timezone', 'port']
        for field in required:
            if field not in new_config:
                return jsonify({'status': 'error', 'message': f'Missing field: {field}'}), 400

        # Validate camera pattern is valid regex
        try:
            re.compile(new_config['camera_pattern'])
        except re.error as e:
            return jsonify({'status': 'error', 'message': f'Invalid camera pattern: {e}'}), 400

        # Validate timezone
        try:
            ZoneInfo(new_config['timezone'])
        except Exception:
            return jsonify({'status': 'error', 'message': f'Invalid timezone: {new_config["timezone"]}'}), 400

        # Validate port
        port = new_config.get('port', 5050)
        if not isinstance(port, int) or port < 1 or port > 65535:
            return jsonify({'status': 'error', 'message': 'Port must be between 1 and 65535'}), 400

        # Save to file
        with open(CONFIG_PATH, 'w') as f:
            json.dump(new_config, f, indent=2)

        # Update runtime config
        CONFIG.update(new_config)
        TIMEZONE = ZoneInfo(CONFIG['timezone'])
        CAMERA_PATTERN = re.compile(CONFIG['camera_pattern'])

        # Clear cache to pick up any pattern changes
        invalidate_session_cache()

        return jsonify({'status': 'ok', 'message': 'Configuration saved'})

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    print(f"\n{'='*50}")
    print("SyncDrive V1")
    print(f"{'='*50}")
    print(f"\nSessions: {SESSIONS_DIR}")
    print(f"Output:   {PROCESSED_DIR}")
    print(f"Config:   {CONFIG_PATH}")
    print(f"\nOpen http://localhost:{CONFIG['port']}\n")
    app.run(host=CONFIG['host'], port=CONFIG['port'], debug=False, threaded=True)
