import discord
from discord.ext import commands
from discord import app_commands
import time
import io
import xml.etree.ElementTree as ET
import os
from dotenv import load_dotenv
from utils.command_list import get_command_list
from stock_analysis.stock_analysis import fetch_and_process_data
from stock_analysis.goldenCrossScrapping import get_goldenCross

# 환경 변수 로드
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

# 디스코드 봇 설정
intents = discord.Intents.default()
intents.members = True  # 서버 멤버 정보
intents.message_content = True  # 메시지 내용

# 봇 정의
bot = commands.Bot(command_prefix="!", intents=intents)

# 봇 준비 후 슬래시 명령어 동기화
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    await bot.tree.sync()  # 슬래시 명령어 동기화

# /명령어목록
@bot.tree.command(name="명령어목록", description="사용 가능한 명령어 목록을 표시합니다.")
async def command_list(interaction: discord.Interaction):
    print("/명령어목록")
    commands_info = get_command_list()
    await interaction.response.send_message(commands_info)

# /주식분석
@bot.tree.command(name="주식분석", description="주식 데이터를 분석하여 결과를 출력합니다.")
async def stock_analysis(interaction: discord.Interaction):
    print("/주식분석")
    start_time = time.time()  # 실행 시작 시간 기록
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))

    # 분석 시작 메시지 전송
    await interaction.response.send_message(f"실행 시간 : {formatted_time} / 분석중...")

    # 주식 데이터 분석 실행
    result_message = fetch_and_process_data(start_time, formatted_time)

    # 2000자 제한을 초과하면 파일로 저장하고, 이내면 그대로 전송
    if len(result_message) > 2000:
        # 파일
        file = io.StringIO(result_message)
        await interaction.followup.send("결과가 너무 길어 파일로 전송합니다.", file=discord.File(file, "stock_analysis_result.txt"))
    else:
        await interaction.followup.send(result_message)

# /골든크로스
@bot.tree.command(name="골든크로스", description="골든크로스 종목 데이터를 출력합니다.")
async def stock_goldenCross(interaction: discord.Interaction):
    print("/골든크로스")
    await interaction.response.defer()
    result_message = get_goldenCross()
    print(result_message)
    if len(result_message) > 2000:
        file = io.StringIO(result_message)
        await interaction.followup.send("결과가 너무 길어 파일로 전송합니다.", file=discord.File(file, "stock_goldenCross_result.txt"))
    else:
        await interaction.followup.send(result_message)

# 봇 실행
if __name__ == '__main__':
    bot.run(TOKEN) 
	