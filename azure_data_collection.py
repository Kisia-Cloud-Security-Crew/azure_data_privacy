import os
import logging
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.cosmos import CosmosClient, exceptions

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_file_permissions(file_path):
    permissions = {
        "readable": os.access(file_path, os.R_OK),
        "writable": os.access(file_path, os.W_OK),
        "executable": os.access(file_path, os.X_OK),
    }
    logging.info(f"파일 권한 확인: {permissions}")
    return permissions

def check_file_type(file_path):
    extension = os.path.splitext(file_path)[1].lower()
    structured_extensions = {".json", ".csv", ".xml"}  # 정형 데이터 확장자
    unstructured_extensions = {".txt", ".docx", ".pdf"}  # 비정형 데이터 확장자

    if extension in structured_extensions:
        file_type = "정형 데이터"
    elif extension in unstructured_extensions:
        file_type = "비정형 데이터"
    else:
        file_type = "알 수 없음"

    logging.info(f"파일 유형 확인: {file_type} ({extension})")
    return file_type

def load_local_data(file_path):
    permissions = check_file_permissions(file_path)
    if not permissions["readable"]:
        logging.error(f"파일을 읽을 수 있는 권한이 없습니다: {file_path}")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.read()
            file_type = check_file_type(file_path)
            logging.info(f"로컬에서 데이터 불러옴: {file_path}, 유형: {file_type}")
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
            raise ValueError("Blob Storage 연결 문자열이 환경 변수에 설정되지 않았습니다.")
        
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
            raise ValueError("Cosmos DB URI 또는 Primary Key가 환경 변수에 설정되지 않았습니다.")

        client = CosmosClient(uri, key)
        logging.info("Cosmos DB 클라이언트가 성공적으로 초기화되었습니다.")
        return client
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB 응답 오류: {e}")
    except Exception as e:
        logging.error(f"Cosmos DB 클라이언트 초기화 오류: {e}")
    return None

def download_blob_data(blob_service_client, container_name, blob_name, download_file_path):
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        with open(download_file_path, "wb") as download_file:
            download_stream = blob_client.download_blob()
            download_file.write(download_stream.readall())
            logging.info(f"Blob에서 로컬로 데이터 다운로드 완료: {download_file_path}")
        
        permissions = check_file_permissions(download_file_path)
        file_type = check_file_type(download_file_path)
        logging.info(f"다운로드한 파일 권한: {permissions}, 유형: {file_type}")

    except Exception as e:
        logging.error(f"Blob 데이터 다운로드 중 오류 발생: {e}")

def main():
    local_file_path = 'local_data.json'
    data = '{"example": "data"}'  # 저장할 예시 데이터
    save_local_data(data, local_file_path)
    loaded_data = load_local_data(local_file_path)
    if loaded_data is None:
        logging.error("로컬 데이터를 로드하는 데 실패했습니다. 프로그램을 종료합니다.")
        return

    blob_service_client = initialize_blob_client()
    if blob_service_client is None:
        logging.error("Blob Storage 클라이언트 초기화에 실패했습니다. 프로그램을 종료합니다.")
        return

    cosmos_client = initialize_cosmos_client()
    if cosmos_client is None:
        logging.error("Cosmos DB 클라이언트 초기화에 실패했습니다. 프로그램을 종료합니다.")
        return

    container_name = 'cloudcloudcontainer'
    blob_name = 'example_blob.json'
    download_file_path = 'downloaded_data.json'
    download_blob_data(blob_service_client, container_name, blob_name, download_file_path)

    logging.info("모든 클라이언트가 성공적으로 초기화되었으며 Blob 데이터가 로컬에 저장되었습니다.")

if __name__ == "__main__":
    main()
