import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.cosmos import CosmosClient, exceptions
from datetime import datetime
import json
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 환경 변수에서 설정 불러오기
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
COSMOS_ENDPOINT = os.getenv("COSMOS_DB_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_DB_KEY")
COSMOS_DATABASE_NAME = os.getenv("COSMOS_DB_NAME")
COSMOS_CONTAINER_NAME = os.getenv("COSMOS_DB_CONTAINER_NAME")

def initialize_blob_client():
    """Blob Storage 클라이언트 초기화"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        return blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
    except Exception as e:
        logging.error(f"Blob 클라이언트 초기화 오류: {str(e)}")
        return None

def initialize_cosmos_client():
    """Cosmos DB 클라이언트 초기화"""
    try:
        client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
        database = client.get_database_client(COSMOS_DATABASE_NAME)
        return database.get_container_client(COSMOS_CONTAINER_NAME)
    except Exception as e:
        logging.error(f"Cosmos DB 클라이언트 초기화 오류: {str(e)}")
        return None

def store_data_in_blob(container_client, data):
    """Blob Storage에 데이터 저장"""
    if not container_client:
        logging.error("Blob 컨테이너 클라이언트가 초기화되지 않았습니다.")
        return

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    blob_name = f'data_{timestamp}.json'
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        logging.info(f'데이터가 Blob Storage에 저장되었습니다. Blob 이름: {blob_name}')
    except Exception as e:
        logging.error(f"Blob Storage 업로드 오류: {str(e)}")

def store_data_in_cosmos(container, data):
    """Cosmos DB에 데이터 저장"""
    if not container:
        logging.error("Cosmos DB 컨테이너 클라이언트가 초기화되지 않았습니다.")
        return

    data['id'] = str(datetime.now().timestamp())
    try:
        container.create_item(body=data)
        logging.info(f'데이터가 Cosmos DB에 저장되었습니다. ID: {data["id"]}')
    except exceptions.CosmosResourceExistsError:
        logging.warning("해당 항목이 이미 Cosmos DB에 존재합니다.")
    except Exception as e:
        logging.error(f"Cosmos DB 데이터 저장 오류: {str(e)}")

def main():
    # 클라이언트 초기화
    blob_container = initialize_blob_client()
    cosmos_container = initialize_cosmos_client()

    if not blob_container or not cosmos_container:
        logging.error("클라이언트 초기화 실패. 프로그램을 종료합니다.")
        return

    # 샘플 데이터
    data = {"timestamp": datetime.now().isoformat(), "value": "샘플 데이터"}

    # 데이터 저장
    store_data_in_blob(blob_container, data)
    store_data_in_cosmos(cosmos_container, data)

if __name__ == "__main__":
    main()
