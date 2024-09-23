import os
import logging
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.cosmos import CosmosClient, exceptions

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_local_data(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.read()
            logging.info(f"로컬에서 데이터 불러옴: {file_path}")
            return data
    except FileNotFoundError:
        logging.error(f"로컬 데이터 파일을 찾을 수 없습니다: {file_path}")
        return None

def save_local_data(data, file_path):
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(data)
            logging.info(f"데이터가 로컬에 저장되었습니다: {file_path}")
    except Exception as e:
        logging.error(f"로컬 데이터 저장 중 오류 발생: {e}")

def initialize_blob_client():
    try:
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not connection_string:
            raise ValueError("Blob Storage 연결 문자열이 없습니다.")
        
        # BlobServiceClient 초기화
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        logging.info("Blob Storage 클라이언트가 성공적으로 초기화되었습니다.")
        return blob_service_client
    except Exception as e:
        logging.error(f"Blob 클라이언트 초기화 오류: {e}")
        return None

def initialize_cosmos_client():
    try:
        uri = os.getenv('COSMOS_DB_URI')
        key = os.getenv('COSMOS_DB_PRIMARY_KEY')
        if not uri or not key:
            raise ValueError("Cosmos DB URI 또는 Primary Key가 없습니다.")

        # CosmosClient 초기화
        client = CosmosClient(uri, key)
        logging.info("Cosmos DB 클라이언트가 성공적으로 초기화되었습니다.")
        return client
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB 응답 오류: {e}")
    except Exception as e:
        logging.error(f"Cosmos DB 클라이언트 초기화 오류: {e}")
    return None

def main():
    # 1. 로컬 데이터 저장 및 불러오기
    local_file_path = 'local_data.json'
    data = '{"example": "data"}'  # 저장할 예시 데이터
    save_local_data(data, local_file_path)
    loaded_data = load_local_data(local_file_path)

    if loaded_data is None:
        logging.error("로컬 데이터를 로드하는 데 실패했습니다. 프로그램을 종료합니다.")
        return

    # 2. Blob Storage 클라이언트 초기화
    blob_service_client = initialize_blob_client()
    if blob_service_client is None:
        logging.error("Blob Storage 클라이언트 초기화에 실패했습니다. 프로그램을 종료합니다.")
        return

    # 3. Cosmos DB 클라이언트 초기화
    cosmos_client = initialize_cosmos_client()
    if cosmos_client is None:
        logging.error("Cosmos DB 클라이언트 초기화에 실패했습니다. 프로그램을 종료합니다.")
        return

    logging.info("모든 클라이언트가 성공적으로 초기화되었습니다.")

if __name__ == "__main__":
    main()
