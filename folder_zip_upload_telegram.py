import os
import asyncio
import zipfile
import logging
from multiprocessing import Manager
from telethon import TelegramClient
import time
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Telegram API credentials
env=load_dotenv()
api_id = os.getenv("api_id")
api_hash = os.getenv("api_hash")
friend_myphone = os.getenv("friend_myphone")
my_phone = os.getenv("my_phone")

#/media/satish/Data/ubuntu_sd4/sda4/NARESH IT FULL STACK DS & AI 2023 - 24  MR. OMKAR

source_folder = "/media/satish/Data/ASHOK IT GENAI"
zip_folder = 'zip_files'
processed_files_log = 'processed_zips.csv'
MAX_ZIP_SIZE = 1.8 * 1024 * 1024 * 1024  # 2GB

client = TelegramClient('178911_satish', api_id, api_hash)

# Ensure zip folder exists
os.makedirs(zip_folder, exist_ok=True)

def get_processed_files():
    try:
        with open(processed_files_log, 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

def store_history(file_path):
    with open(processed_files_log, 'a') as file:
        file.write(f"{file_path}\n")

def create_zip_chunks(folder_path):
    base_name = os.path.basename(folder_path)
    zip_files = []
    part = 1
    
    logging.info("Starting ZIP creation...")
    
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)]
    chunk_size = 0
    zip_file_path = os.path.join(zip_folder, f"{base_name}_part{part}.zip")
    zipf = zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)
    
    for file in files:
        file_size = os.path.getsize(file)
        if chunk_size + file_size > MAX_ZIP_SIZE:
            zipf.close()
            zip_files.append(zip_file_path)
            logging.info(f"Created ZIP: {zip_file_path}")
            
            part += 1
            zip_file_path = os.path.join(zip_folder, f"{base_name}_part{part}.zip")
            zipf = zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)
            chunk_size = 0
        
        zipf.write(file, os.path.basename(file))
        chunk_size += file_size
    
    zipf.close()
    zip_files.append(zip_file_path)
    logging.info(f"Created ZIP: {zip_file_path}")
    
    return zip_files

async def upload_worker(queue):
    await client.start(my_phone)
    friend = await client.get_entity(friend_myphone)
    logging.info("Upload worker started.")
    
    while True:
        zip_file_path = await queue.get()
        if zip_file_path is None:
            logging.info("No more files to upload. Exiting worker.")
            break  # Exit when done
        
        try:
            logging.info(f"Uploading: {zip_file_path}")
            await client.send_file(friend, zip_file_path, caption=os.path.basename(zip_file_path))
            await time.sleep(100)
            logging.info(f"Upload completed: {zip_file_path}")
            store_history(zip_file_path)
            os.remove(zip_file_path)
        except Exception as e:
            logging.error(f"Upload failed for {zip_file_path}: {e}")
        finally:
            queue.task_done()

    await client.disconnect()
    logging.info("Telegram client disconnected.")

async def main():
    processed_files = get_processed_files()
    
    if source_folder in processed_files:
        logging.info("Skipping already processed folder.")
        return
    
    logging.info("Starting ZIP processing and upload...")
    
    manager = Manager()
    queue = asyncio.Queue()
    
    upload_task = asyncio.create_task(upload_worker(queue))
    
    zip_files = create_zip_chunks(source_folder)
    
    for zip_file in zip_files:
        logging.info(f"Adding {zip_file} to upload queue")
        await queue.put(zip_file)
    
    await queue.join()
    await queue.put(None)
    
    await upload_task
    store_history(source_folder)
    
    logging.info("Process completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())