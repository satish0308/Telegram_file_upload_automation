import os
import asyncio
import subprocess
import multiprocessing
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import DocumentAttributeVideo 
import shutil
from dotenv import load_dotenv


# Configurations
file_folder = "/media/satish/Data/ubuntu_sd4/sda4/NARESH IT FULL STACK DS & AI 2023 - 24  MR. OMKAR"
env=load_dotenv()
api_id = os.getenv("api_id")
api_hash = os.getenv("api_hash")
friend_myphone = os.getenv("friend_myphone")
my_phone = os.getenv("my_phone")

process_file_folder = "process_files"
num_processes = 3 # Adjust based on CPU cores

# Initialize Telegram client
client = TelegramClient('17890_satish', api_id, api_hash)

# Ensure process folder exists
os.makedirs(process_file_folder, exist_ok=True)

def progress_callback(current, total):
    """Callback function to display upload progress."""
    percent = (current / total) * 100
    print(f"Uploading... {current}/{total} bytes ({percent:.2f}%)")

def process_video(input_path, output_path,process=True):
    global process_file_folder
    if process:
        """Processes a video and formats it for streaming."""
        command = [
            'ffmpeg', '-i', input_path,
            '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            '-profile:v', 'baseline', '-level', '3.0',
            '-f', 'mp4', output_path
        ]
        try:
            subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return output_path
        except subprocess.CalledProcessError as e:
            print(f"Error processing video {input_path}: {e}")
            return None
    else:
        shutil.copy(input_path, process_file_folder)

def store_history(file_path):
    """Store processed file history."""
    with open('processed_files.csv', 'a') as file:
        file.write(f"{file_path}\n")

def get_processed_files():
    """Retrieve list of already processed files."""
    try:
        with open('processed_files.csv', 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

async def send_video(queue):
    """Asynchronously sends processed videos to Telegram."""
    await client.start(my_phone)
    friend = await client.get_entity(friend_myphone)
    
    while not queue.empty():
        processed_file_path, original_file_path, folder_name = await queue.get()
        
        try:
            ffprobe_command = [
                'ffprobe', '-v', 'error', '-select_streams', 'v:0',
                '-show_entries', 'stream=duration,width,height',
                '-of', 'default=noprint_wrappers=1:nokey=1', processed_file_path
            ]
            ffmpeg_output = subprocess.check_output(ffprobe_command).decode().split('\n')

            if len(ffmpeg_output) >= 3:
                try:
                    duration = float(ffmpeg_output[2].strip())
                    width = int(ffmpeg_output[0].strip())
                    height = int(ffmpeg_output[1].strip())
                except ValueError:
                    print(f"Skipping {processed_file_path}, invalid metadata.")
                    continue
            else:
                print(f"Skipping {processed_file_path}, ffprobe missing data.")
                continue

            video_attribute = DocumentAttributeVideo(
                duration=duration, w=width, h=height, supports_streaming=True
            )

            print(f"Sending: {processed_file_path}")
            await client.send_message(friend, folder_name)
            await client.send_file(friend, processed_file_path, attributes=[video_attribute], caption=os.path.basename(original_file_path), force_document=False, progress_callback=progress_callback)

            store_history(processed_file_path)
            os.remove(processed_file_path)
        
        except Exception as e:
            print(f"Failed to send {processed_file_path}: {e}")

async def main():
    """Main function to process and send videos in parallel."""
    processed_files = get_processed_files()
    queue = asyncio.Queue()
    
    # Collect videos to process
    video_files = []
    for root, _, files in os.walk(file_folder):
        for file in files:
            if file.endswith('.mp4'):
                file_path = os.path.join(root, file)
                processed_file_path = os.path.join(process_file_folder, file.replace('.mp4', '_processed.mp4'))
                if processed_file_path not in processed_files:
                    processed_file_path = os.path.join(process_file_folder, file.replace('.mp4', '_processed.mp4'))
                    video_files.append((file_path, processed_file_path, root.split('/')[-1]))

    # Use multiprocessing to process videos in parallel
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.starmap(process_video, [(file, processed,False) for file, processed, _ in video_files])
    
    # Add processed files to queue for uploading
    for i, (original_file, processed_file, folder_name) in enumerate(video_files):
        if results[i]:
            await queue.put((processed_file, original_file, folder_name))

    # Start Telegram file sending
    await send_video(queue)

with client:
    client.loop.run_until_complete(main())
