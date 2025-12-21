import random
from datetime import datetime, timedelta
import argparse

def generate_sample_nem12(filename, nmi_count=5, days=30):
    print(f"Generating sample NEM12 data: {filename} with {nmi_count} NMIs for {days} days each.")
    with open(filename, 'w') as f:
        # 100 Header
        f.write(f"100,NEM12,{datetime.now().strftime('%Y%m%d%H%M')},GEN_TOOL,LOCAL\n")
        
        for i in range(nmi_count):
            nmi = f"NMI{1000000 + i}"
            interval = random.choice([5, 15, 30]) # hybrid intervals
            f.write(f"200,{nmi},E1,1,E1,N1,METER{i},kWh,{interval},20250101\n")
            
            start_date = datetime(2025, 1, 1)
            for d in range(days):
                current_date = start_date + timedelta(days=d)
                # compute number of data points
                points_count = 1440 // interval
                readings = [f"{random.uniform(0.1, 2.0):.3f}" for _ in range(points_count)]
                
                # 300 Record
                line = f"300,{current_date.strftime('%Y%m%d')},{','.join(readings)},A,,,20250101000000\n"
                f.write(line)
        
        # 900 Footer
        f.write("900\n")
        print("Sample NEM12 data generation complete.")

def parse_args():
    p = argparse.ArgumentParser(description="generate sample NEM12 load data")
    p.add_argument('--nmi_count', type=int, default=5, help='NMI count(default 5)')
    p.add_argument('--days', type=int, default=30, help='Report days per NMI(default 30)')
    p.add_argument('--output', '-o', type=str, default='../test_load.csv', help='output file path（default ../test_load.csv)')
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    min_count = max(1, args.nmi_count)
    days = max(1, args.days)
    generate_sample_nem12(args.output, nmi_count=min_count, days=days)
