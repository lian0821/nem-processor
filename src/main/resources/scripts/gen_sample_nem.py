import random
from datetime import datetime, timedelta

def generate_sample_nem12(filename, nmi_count=5, days=30):
    with open(filename, 'w') as f:
        # 100 Header
        f.write(f"100,NEM12,{datetime.now().strftime('%Y%m%d%H%M')},GEN_TOOL,LOCAL\n")
        
        for i in range(nmi_count):
            nmi = f"NMI{1000000 + i}"
            interval = random.choice([5, 15, 30]) # 混合频率测试
            f.write(f"200,{nmi},E1,1,E1,N1,METER{i},kWh,{interval},20250101\n")
            
            start_date = datetime(2025, 1, 1)
            for d in range(days):
                current_date = start_date + timedelta(days=d)
                # 计算该间隔下一天应有的点数
                points_count = 1440 // interval
                readings = [f"{random.uniform(0.1, 2.0):.3f}" for _ in range(points_count)]
                
                # 300 Record
                line = f"300,{current_date.strftime('%Y%m%d')},{','.join(readings)},A,,,20250101000000\n"
                f.write(line)
        
        # 900 Footer
        f.write("900\n")

generate_sample_nem12("../test_load.csv", nmi_count=10, days=365)
