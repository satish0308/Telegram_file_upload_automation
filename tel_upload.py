import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import subprocess
from telethon.tl.types import DocumentAttributeVideo 
import shutil
from dotenv import load_dotenv

file_folder = "/media/satish/Data/ubuntu_sd4/sda4/NARESH IT FULL STACK DS & AI 2023 - 24  MR. OMKAR"
env=load_dotenv()
api_id = os.getenv("api_id")
api_hash = os.getenv("api_hash")
friend_myphone = os.getenv("friend_myphone")
my_phone = os.getenv("my_phone")


prcess_file_folder='process_files'
client = TelegramClient('1789_satish', api_id, api_hash)

# Define the progress callback function
def progress_callback(current, total):
    """Callback function to display upload progress."""
    percent = (current / total) * 100
    print(f"Uploading... {current}/{total} bytes ({percent:.2f}%)")

# def process_video(input_path, output_path):
#     """Process the video and format it for streaming."""
#     # FFmpeg command to move the moov atom for progressive download
#     command = [
#         'ffmpeg', '-i', input_path,
#         '-movflags', '+faststart',  # Move moov atom to the beginning
#         '-c:v', 'libx264',  # Use H.264 video codec
#         '-c:a', 'aac',  # Use AAC audio codec
#         '-strict', 'experimental',  # Allow experimental codecs
#         output_path  # Output file path
#     ]
#     subprocess.run(command, check=True)  # Run the FFmpeg command

# def process_video(input_path, output_path):
#     """Process the video and format it for streaming."""
#     command = [
#         'ffmpeg', '-i', input_path,
#         '-c:v', 'libx264',  # Video codec H.264
#         '-preset', 'fast',  # Faster encoding preset
#         '-crf', '23',  # Quality factor (lower is better quality)
#         '-c:a', 'aac',  # Audio codec AAC
#         '-b:a', '128k',  # Audio bitrate 128k
#         '-movflags', '+faststart',  # Move the moov atom to the beginning
#         '-profile:v', 'baseline',  # Ensure wide compatibility
#         '-level', '3.0',  # Set level for streaming compatibility
#         '-f', 'mp4',  # Force MP4 output format
#         output_path  # Output file path
#     ]
#     subprocess.run(command, check=True)  #

def process_video(input_path, output_path,process=True):
    global prcess_file_folder
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
        shutil.copy(input_path, prcess_file_folder)

def store_history(full_file_path):
    """Store the history of processed files."""
    with open('processed_files.csv', 'a') as file:
        file.write(f"{full_file_path}\n")

def get_list_of_files():
    """Get the list of processed files."""
    try:
        with open('processed_files.csv', 'r') as file:
            return file.read().splitlines()
    except FileNotFoundError:
        return []

async def main():
    await client.start(my_phone)
    friend = await client.get_entity(friend_myphone)
    print(f"Found friend: {friend.username if friend.username else friend.id}")
    
    for main_file_folder,sub_folder,all_file in os.walk(file_folder):
        # print(main_file_folder,sub_folder,all_file)
        print("Processing files...")
        for file in all_file:
            file_path = os.path.join(main_file_folder, file)
            # print(file_path)
            if file.endswith('.mp4') and  file_path not in get_list_of_files():
                processed_file = file.replace('.mp4', '_processed.mp4')
                # processed_file=processed_file.replace(' ','_')
                processed_file_path = os.path.join(prcess_file_folder, processed_file)
                if not os.path.exists(prcess_file_folder):
                    os.mkdirs(prcess_file_folder)
                print(f"Processing video: {file_path}")
                try:
                    process_video(file_path, processed_file_path,process=False)

                    ffprobe_command = [
                        'ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries',
                        'stream=duration,width,height', '-of', 'default=noprint_wrappers=1:nokey=1', processed_file_path
                    ]
                    ffmpeg_output = subprocess.check_output(ffprobe_command).decode().split('\n')
                    print(f"FFprobe Output: {ffmpeg_output}")  # Debugging the output

                    # Check if we have valid values for duration, width, and height
                    if len(ffmpeg_output) >= 3:
                        try:
                            duration = float(ffmpeg_output[2].strip())  # Video duration in seconds (float)
                            width = int(ffmpeg_output[0].strip())  # Video width
                            height = int(ffmpeg_output[1].strip())  # Video height
                        except ValueError as e:
                            print(f"Error converting values: {e}")
                            continue  # Skip this file if conversion fails
                    else:
                        print("FFprobe output is missing expected values.")
                        continue  # Skip if FFprobe didn't return expected output

                    video_attribute = DocumentAttributeVideo(
                        duration=duration,
                        w=width,    
                        h=height,
                        supports_streaming=True
                    )

                    
                    print(f"Video processed: {processed_file_path}")
                    print(f"Sending the file {processed_file_path}")
                    await client.send_message(friend, main_file_folder.split(r'/')[-1])
                    await client.send_file(friend, processed_file_path, attributes=(video_attribute,),caption=file,force_document = False, progress_callback=progress_callback)

                    # await client.send_file(friend, processed_file_path,caption=file,progress_callback=progress_callback)
                    print(f"Sending the file {processed_file_path}")
                    store_history(file_path)
                    
                except Exception as e:
                    print(f"Failed to send file {processed_file_path}: {e}")
                finally:
                    if os.path.exists(processed_file_path):
                        os.remove(processed_file_path)
        
    print("Done sending files.")

with client:
    client.loop.run_until_complete(main())
