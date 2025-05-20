import time
import condition_ce

if __name__ == "__main__":
    while True:
        try:
            condition_ce.condition()
        except Exception as e:
            print(f"Error in main loop: {e}")
        time.sleep(300) 