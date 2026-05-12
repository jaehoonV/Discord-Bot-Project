import asyncio
from datetime import datetime
import pytz

KST = pytz.timezone('Asia/Seoul')

def is_market_day() -> bool:
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    return True

async def daily_stock_scheduler(bot, channel_id: int):
    await bot.wait_until_ready()

    while not bot.is_closed():
        now = datetime.now(KST)

        # 08:40분 자동분석
        target = now.replace(hour=8, minute=40, second=0, microsecond=0) 
        if now >= target:
            # 이미 지났으면 다음날 08:40 대기
            target = target.replace(day=target.day + 1)

        wait_seconds = (target - now).total_seconds()
        await asyncio.sleep(wait_seconds)

        # 장날 여부 확인
        if not is_market_day():
            print(f"[스케줄러] {datetime.now(KST).strftime('%Y-%m-%d')} 휴장일 스킵")
            continue

        # 분석 실행 및 채널 전송
        try:
            import time, io
            import discord
            from stock_analysis.stock_analysis import fetch_and_process_data

            channel = bot.get_channel(channel_id)
            if channel is None:
                print(f"[스케줄러] 채널 {channel_id} 없음")
                continue

            start_time = time.time()
            formatted_time = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

            await channel.send(f"[자동 분석] {formatted_time} 분석 시작...")

            # fetch_and_process_data는 blocking → run_in_executor로 비동기 처리
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, fetch_and_process_data, start_time, formatted_time
            )

            if len(result) > 2000:
                file = io.StringIO(result)
                await channel.send(
                    "분석 완료 (파일 첨부)",
                    file=discord.File(file, "auto_stock_analysis.txt")
                )
            else:
                await channel.send(result)

        except Exception as e:
            print(f"[스케줄러 오류] {e}")