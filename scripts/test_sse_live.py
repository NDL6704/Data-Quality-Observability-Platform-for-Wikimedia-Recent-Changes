"""Script kiểm tra thực tế việc đọc stream từ Wikimedia."""

import sys
from pathlib import Path

# Đảm bảo in unicode tiếng Việt an toàn trên console Windows
if sys.stdout.encoding != "utf-8":
    try:
        reconfig = getattr(sys.stdout, "reconfigure", None)
        if callable(reconfig):
            reconfig(encoding="utf-8")
    except Exception:
        pass

# Tự động định vị và thêm thư mục gốc của dự án vào sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.sse_client import WikimediaSSEClient  # noqa: E402


def main() -> None:
    print("Khởi tạo Wikimedia SSE Client...")
    client = WikimediaSSEClient()

    count = 0
    max_events = 5  # Đọc thử 5 event đầu tiên rồi dừng

    try:
        for event in client.stream_events():
            count += 1
            print(f"\n--- [Event #{count}] ---")
            print(f"Event ID : {event.event_id}")
            print(f"Wiki     : {event.data.get('wiki')}")
            print(f"Title    : {event.data.get('title')}")
            print(f"User     : {event.data.get('user')}")
            print(f"Type     : {event.data.get('type')}")
            print(f"Timestamp: {event.data.get('timestamp')}")

            if count >= max_events:
                print("\nĐã nhận đủ 5 events mẫu thành công!")
                client.stop()
                break
    except KeyboardInterrupt:
        client.stop()


if __name__ == "__main__":
    main()
