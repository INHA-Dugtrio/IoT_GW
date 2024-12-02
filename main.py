import asyncio
import struct
from datetime import datetime
import ipaddress
import pika
import json

# TCP 서버 설정
HOST = '0.0.0.0'
PORT = 3000
AI_HOST = ''

# RabbitMQ 설정
RABBITMQ_HOST = ''
RABBITMQ_USER = ''
RABBITMQ_PASSWORD = ''

# 데이터 포맷 및 크기 정의
SENSOR_DATA_FORMAT = 'iiQ6f'
SENSOR_DATA_SIZE = struct.calcsize(SENSOR_DATA_FORMAT)

# 스택 초기화 및 최대 크기 설정
MAX_STACK_SIZE = 1000
data_stack = []
stack_lock = asyncio.Lock()

# RabbitMQ 연결 생성
def get_rabbitmq_channel():
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
        )
        channel = connection.channel()
        return channel, connection
    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")
        return None, None

rabbitmq_channel, rabbitmq_connection = get_rabbitmq_channel()

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    client_ip, client_port = addr
    print(f"Connected by {addr}")

    try:
        while True:
            # AI_HOST 처리: 스택에서 데이터 pop 후 전송
            if ipaddress.ip_address(client_ip) == ipaddress.ip_address(AI_HOST):
                async with stack_lock:
                    if data_stack:  # 스택이 비어 있지 않은 경우
                        data_to_send = data_stack.pop()
                    else:
                        data_to_send = None

                if data_to_send:
                    try:
                        writer.write(data_to_send)
                        await writer.drain()
                        print("Sent data from stack")
                    except Exception as e:
                        print(f"Error sending data to {addr}: {e}")
                        break
                else:
                    await asyncio.sleep(0.1)  # Prevent busy-waiting
            else:
                # 데이터를 수신
                data = await reader.read(SENSOR_DATA_SIZE)
                if not data:
                    break

                async with stack_lock:
                    if len(data_stack) >= MAX_STACK_SIZE:
                        data_stack.pop(0)  # 오래된 데이터 제거
                    data_stack.append(data)

                # 데이터를 파싱
                try:
                    unpacked_data = struct.unpack(SENSOR_DATA_FORMAT, data)
                    factory_id, device_id, timeStamp, vibration_x, vibration_y, vibration_z, voltage, rpm, temperature = unpacked_data

                    # 공통 데이터
                    common_data = {
                        "factory_id": factory_id,
                        "device_id": device_id,
                        "timeStamp": timeStamp,
                    }

                    # 센서 데이터 및 큐 이름 매핑
                    sensor_data = {
                        "vibration_x": vibration_x,
                        "vibration_y": vibration_y,
                        "vibration_z": vibration_z,
                        "voltage": voltage,
                        "rpm": rpm,
                        "temperature": temperature,
                    }

                    for sensor, value in sensor_data.items():
                        queue_name = f"{sensor}_queue"  # 센서 이름으로 큐 생성

                        # 큐 선언
                        try:
                            rabbitmq_channel.queue_declare(queue=queue_name, durable=True)

                            # 메시지 구성
                            message = {
                                **common_data,  # 공통 데이터 추가
                                "sensor_type": sensor,
                                "value": value,
                            }

                            # 메시지 전송
                            rabbitmq_channel.basic_publish(
                                exchange="",
                                routing_key=queue_name,
                                body=json.dumps(message),
                            )
                            print(f"Published to {queue_name}: {message}")
                        except Exception as e:
                            print(f"Error declaring or publishing to {queue_name}: {e}")

                except struct.error as e:
                    print(f"Error unpacking data: {e}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        print(f"Disconnected from {addr}")
        writer.close()
        await writer.wait_closed()


async def start_server():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"Server listening on {HOST}:{PORT}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(start_server())
    except KeyboardInterrupt:
        print("Shutting down server...")
    finally:
        # RabbitMQ 연결 닫기
        if rabbitmq_channel:
            rabbitmq_channel.close()
        if rabbitmq_connection:
            rabbitmq_connection.close()
