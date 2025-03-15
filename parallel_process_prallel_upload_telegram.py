import os
import asyncio
import subprocess
from multiprocessing import Pool, Manager
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeVideo
from dotenv import load_dotenv


# Telegram API credentials
env=load_dotenv()
api_id = os.getenv("api_id")
api_hash = os.getenv("api_hash")
friend_myphone = os.getenv("friend_myphone")
my_phone = os.getenv("my_phone")


#/media/satish/Data/ubuntu_sd4/sda4/PYSPARK BY DURGASOFT

file_folder = "/media/satish/Data/ubuntu_sd4/sda4/NARESH IT FULL STACK DS & AI 2023 - 24  MR. OMKAR"
process_file_folder = 'process_files'
processed_files_log = 'processed_files.csv'

client = TelegramClient('1789_satish', api_id, api_hash)

# Ensure process folder exists
os.makedirs(process_file_folder, exist_ok=True)

# Progress callback for uploads
def progress_callback(current, total):
    percent = (current / total) * 100
    print(f"Uploading... {current}/{total} bytes ({percent:.2f}%)")

# Function to fetch processed files history
def get_processed_files():
    try:
        with open(processed_files_log, 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

# Function to store processed files
def store_history(file_path):
    with open(processed_files_log, 'a') as file:
        file.write(f"{file_path}\n")

# Function to process video
def process_video(file_path):
    processed_file = os.path.join(process_file_folder, os.path.basename(file_path).replace('.mp4', '_processed.mp4'))
    
    # Skip processing if already processed
    if processed_file in processed_files:
        print(f"Skipping already processed file: {file_path}")
        return None
    
    command = [
        'ffmpeg', '-i', file_path,
        '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
        '-c:a', 'aac', '-b:a', '128k', '-movflags', '+faststart',
        '-profile:v', 'baseline', '-level', '3.0', '-f', 'mp4', processed_file
    ]
    try:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return processed_file  # Return processed file path for uploading
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

# Upload function (async)
async def upload_worker(queue):
    await client.start(my_phone)
    friend = await client.get_entity(friend_myphone)
    while True:
        processed_file_path = await queue.get()
        if processed_file_path is None:
            break  # Exit when done
        
        try:
            # Extract video metadata using ffprobe
            ffprobe_command = [
                'ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries',
                'stream=width,height,duration', '-of', 'default=noprint_wrappers=1:nokey=1', processed_file_path
            ]
            ffmpeg_output = subprocess.check_output(ffprobe_command).decode().split('\n')
            if len(ffmpeg_output) >= 3:
                width, height, duration = map(str.strip, ffmpeg_output[:3])
                duration = float(duration) if duration else 0.0
                width, height = int(width), int(height)
            else:
                raise ValueError("FFprobe output incomplete")
            
            video_attribute = DocumentAttributeVideo(
                duration=duration, w=width, h=height, supports_streaming=True
            )
            
            # Send file
            await client.send_file(
                friend, processed_file_path,
                attributes=[video_attribute], caption=os.path.basename(processed_file_path),
                force_document=False, progress_callback=progress_callback
            )
            print(f"Uploaded: {processed_file_path}")
            store_history(processed_file_path)
            os.remove(processed_file_path)
            print(f"Deleted: {processed_file_path}")
        except Exception as e:
            print(f"Upload failed for {processed_file_path}: {e}")
        finally:
            queue.task_done()

# Main function
def main():
    global processed_files
    processed_files = get_processed_files()
    
    files_to_process = [os.path.join(root, f) for root, _, files in os.walk(file_folder) for f in files if f.endswith('.mp4')]
    files_to_process = [f for f in files_to_process if os.path.join(process_file_folder, os.path.basename(f).replace('.mp4', '_processed.mp4')) not in processed_files]
    
    if not files_to_process:
        print("No new files to process.")
        return
    
    manager = Manager()
    queue = asyncio.Queue()
    
    # Start async upload task
    loop = asyncio.get_event_loop()
    loop.create_task(upload_worker(queue))
    
    # Parallel processing
    with Pool(processes=8) as pool:
        for processed_file in pool.imap_unordered(process_video, files_to_process):
            if processed_file:
                loop.call_soon_threadsafe(queue.put_nowait, processed_file)
    
    # Wait for uploads to complete
    loop.run_until_complete(queue.join())
    loop.call_soon_threadsafe(queue.put_nowait, None)
    loop.run_until_complete(client.disconnect())

if __name__ == "__main__":
    asyncio.run(main())