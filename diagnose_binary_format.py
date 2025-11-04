#!/usr/bin/env python3
"""
Diagnose Binary Format Issues
Analyzes binary files to detect timestamp-as-token misalignment
"""

from pathlib import Path
import struct
from datetime import datetime

def analyze_binary_file(file_path: Path, sample_size: int = 100):
    """Analyze binary file to detect format issues"""
    
    print(f"Analyzing: {file_path.name}")
    print(f"Size: {file_path.stat().st_size / (1024**2):.2f} MB")
    print("="*70)
    
    with open(file_path, 'rb') as f:
        data = f.read()
    
    # Check for websocket frame format
    if len(data) >= 4:
        try:
            num_packets = struct.unpack(">H", data[0:2])[0]
            first_length = struct.unpack(">H", data[2:4])[0]
            if 0 < num_packets < 10000 and first_length in (184, 44, 32, 8):
                print(f"WebSocket frame format detected: {num_packets} packets")
                return analyze_websocket_format(data, sample_size)
        except:
            pass
    
    # Try raw packet stream
    print("Raw packet stream format")
    return analyze_raw_packets(data, sample_size)

def analyze_websocket_format(data: bytes, sample_size: int):
    """Analyze websocket frame format"""
    position = 2  # Skip num_packets header
    samples = []
    
    while position + 4 < len(data) and len(samples) < sample_size:
        if position + 2 > len(data):
            break
        packet_length = struct.unpack(">H", data[position:position+2])[0]
        position += 2
        
        if packet_length not in (184, 44, 32, 8) or position + packet_length > len(data):
            break
        
        packet_data = data[position:position+packet_length]
        samples.append((packet_length, packet_data))
        position += packet_length
    
    return analyze_packet_samples(samples)

def analyze_raw_packets(data: bytes, sample_size: int):
    """Analyze raw packet stream"""
    position = 0
    samples = []
    
    while position < len(data) and len(samples) < sample_size:
        parsed = False
        for packet_length in (184, 44, 32, 8):
            end_pos = position + packet_length
            if end_pos > len(data):
                continue
            
            packet_data = data[position:end_pos]
            samples.append((packet_length, packet_data))
            position = end_pos
            parsed = True
            break
        
        if not parsed:
            position += 1
    
    return analyze_packet_samples(samples)

def analyze_packet_samples(samples: list):
    """Analyze packet samples for misalignment"""
    print(f"\nAnalyzing {len(samples)} packet samples...")
    
    timestamp_as_token_count = 0
    zero_token_count = 0
    valid_token_count = 0
    
    MIN_EPOCH = 1577836800  # Jan 1, 2020
    MAX_EPOCH = 1767225600  # Jan 1, 2026
    
    for packet_length, packet_data in samples[:100]:  # Sample first 100
        if packet_length == 184:
            # Full mode packet
            if len(packet_data) >= 64:
                fields = struct.unpack(">IIIIIIIIIIIIIIII", packet_data[:64])
                token = fields[0]
                timestamp_field_11 = fields[11]  # last_traded_timestamp
                timestamp_field_15 = fields[15]  # exchange_timestamp
                
                # Check if token looks like a timestamp
                if MIN_EPOCH <= token <= MAX_EPOCH:
                    timestamp_as_token_count += 1
                    try:
                        dt = datetime.fromtimestamp(token)
                        if timestamp_as_token_count <= 5:
                            print(f"  ⚠️  Token {token:,} = {dt} (looks like timestamp!)")
                            print(f"      Field[11] (last_traded): {timestamp_field_11}")
                            print(f"      Field[15] (exchange): {timestamp_field_15}")
                    except:
                        pass
                elif token == 0:
                    zero_token_count += 1
                elif 1 <= token <= 1000000000:  # Reasonable token range
                    valid_token_count += 1
    
    print(f"\nSummary:")
    print(f"  Valid tokens: {valid_token_count}")
    print(f"  Zero tokens: {zero_token_count}")
    print(f"  Timestamp-as-token: {timestamp_as_token_count}")
    
    if timestamp_as_token_count > valid_token_count:
        print(f"\n⚠️  ISSUE DETECTED: Many packets have timestamps as tokens!")
        print(f"   This suggests binary format misalignment")
        return True
    
    return False

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Diagnose binary format issues")
    parser.add_argument("files", nargs="+", help="Binary files to analyze")
    parser.add_argument("--sample", type=int, default=100, help="Number of packets to sample")
    
    args = parser.parse_args()
    
    issues_found = []
    
    for file_path_str in args.files:
        file_path = Path(file_path_str)
        if not file_path.exists():
            print(f"❌ File not found: {file_path}")
            continue
        
        has_issue = analyze_binary_file(file_path, args.sample)
        if has_issue:
            issues_found.append(file_path)
        print()
    
    if issues_found:
        print("="*70)
        print(f"⚠️  Issues found in {len(issues_found)} files:")
        for f in issues_found:
            print(f"   - {f}")
    else:
        print("="*70)
        print("✅ No format issues detected in analyzed files")

if __name__ == "__main__":
    main()

