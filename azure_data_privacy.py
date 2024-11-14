import csv
import os
import re
import logging
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient, exceptions
import mysql.connector

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 민감 데이터 패턴
patterns = {
    "주민등록번호": r"(\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])[-\s]*[1-4][\d*]{6})",
    "휴대전화 및 집 전화": r"((?:\(?0(?:[1-9]{2})\)?[-\s]?)?(?:\d{3,4})[-\s]?\d{4})",
    "계좌/카드번호": r"((\d{4}[-\s]?\d{6}[-\s]?\d{4,5})|(\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}))",
    "이메일": r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4})",
    "사업자등록번호": r"((1[1-6]|[2-9][0-9])-?(\d{2})-?\d{5})",
    "외국인등록번호": r"(\d{2}[01]\d[0123]\d[-\s]?[5678]\d{6})",
    "여권번호": r"([DMORS]\d{8}|(AY|BS|CB|CS|DG|EP|GB|GD|GG|GJ|GK|GN|GP|GS|GW|GY|HD|IC|JB|JG|JJ|JN|JR|JU|KJ|KN|KR|KY|MP|NW|SC|SJ|SM|SQ|SR|TJ|TM|UL|YP|YS)\d{7})",
    "운전면허": r"(서울|대전|대구|부산|광주|울산|인천|제주|강원|경기|충북|충남|전남|전북|경북|경남)[\s]{0,2}\d{2}[-\s]?\d{6}[-\s]?\d{2}",
    "건강보험번호": r"([1-9]-[0-6]\d{9})"
}

# 민감 데이터 검출 함수
def detect_sensitive_data(text):
    detected_data = {}
    for data_type, pattern in patterns.items():
        matches = re.findall(pattern, text)
        if matches:
            detected_data[data_type] = matches
    return detected_data

# CSV 파일로 검출 결과 저장 함수
def save_sensitive_data_to_csv(results, output_path='sensitive_data_report.csv'):
    with open(output_path, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['테이블', '컬럼', '민감 데이터 유형', '민감 여부', '내용'])

        for location, sensitive_data in results.items():
            table_column = location.split('.')
            table = table_column[0]
            column = table_column[1]
            for data_type, matches in sensitive_data.items():
                for match in matches:
                    writer.writerow([table, column, data_type, "민감", match])
                    # 콘솔에 출력
                    logging.info(f"테이블: {table}, 컬럼: {column}, 데이터: {match}, 민감 데이터 유형: {data_type}")
    logging.info(f"민감 데이터 보고서가 CSV 파일로 저장되었습니다: {output_path}")

# Blob Storage에 파일 업로드 함수
def save_to_blob_storage(filename, content):
    blob_service_client = BlobServiceClient.from_connection_string(os.getenv("BLOB_CONNECTION_STRING"))
    container_name = "sensitive-data-reports"
    container_client = blob_service_client.get_container_client(container_name)
    
    # Blob 클라이언트 생성 및 파일 업로드
    blob_client = container_client.get_blob_client(filename)
    blob_client.upload_blob(content, overwrite=True)
    logging.info(f"민감 데이터 보고서가 Blob Storage에 저장되었습니다: {filename}")

# 데이터베이스에서 민감 데이터 검출
def scan_database():
    # 환경 변수로부터 데이터베이스 구성 가져오기
    db_config = {
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', 'tpgus112401216!'),
        'host': os.getenv('DB_HOST', 'DESKTOP-DUQPAKF'),
        'database': os.getenv('DB_NAME', 'my_database')
    }
    
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    results = {}

    for table in tables:
        cursor.execute(f"DESCRIBE {table}")
        columns = [column[0] for column in cursor.fetchall()]
        
        for column in columns:
            cursor.execute(f"SELECT {column} FROM {table}")
            rows = cursor.fetchall()
            
            for row in rows:
                if row[0] is not None:
                    text = str(row[0])
                    detected = detect_sensitive_data(text)
                    if detected:
                        results[f"{table}.{column}"] = detected
    cursor.close()
    conn.close()
    return results

# 실행
scan_results = scan_database()
csv_filename = 'sensitive_data_report.csv'
save_sensitive_data_to_csv(scan_results, output_path=csv_filename)

# CSV 파일을 Blob Storage에 업로드
with open(csv_filename, 'rb') as f:
    save_to_blob_storage(csv_filename, f)

logging.info("민감 데이터 검출 및 보고서 작성 완료")
