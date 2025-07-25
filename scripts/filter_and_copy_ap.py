import os
import shutil
from datetime import datetime
import re

# 設定項目

# 新しい買掛金プロジェクトのルートディレクトリ
NEW_AP_PROJECT_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead_apProcessor'

# コピー元フォルダ（教師データがある場所）
SOURCE_BASE_DIR = r'G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv'

# コピー先フォルダ (新しい共有ドライブのパス)
DEST_INPUT_DIR = r'G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\40_買掛金\Import'

# 抽出対象のファイル名パターン (B*091.csv)
TARGET_FILE_PATTERN = r'^B\d{6}_.*\.jpg_091\.csv$' # Bxxxxxx_*.jpg_091.csv の形式にマッチ

# --- 関数定義 ---
def filter_and_copy_csv_files():
    """
    指定されたソースディレクトリから特定のパターンに合致するCSVファイルを抽出し、
    新しい買掛金プロジェクトのinputディレクトリにコピー
    """
    print(f"--- 買掛金CSVファイル抽出・コピー処理開始 ({datetime.now()}) ---")
    print(f"コピー元フォルダ: {SOURCE_BASE_DIR}")
    print(f"コピー先フォルダ: {DEST_INPUT_DIR}")

    # コピー先フォルダが存在しない場合は作成
    os.makedirs(DEST_INPUT_DIR, exist_ok=True)

    copied_files_count = 0

    # ソースディレクトリ内を再帰的に検索
    for root, dirs, files in os.walk(SOURCE_BASE_DIR):
        for filename in files:
            # ファイル名がターゲットパターンに合致するかチェック
            if filename.lower().endswith('.csv') and re.match(TARGET_FILE_PATTERN, filename, re.IGNORECASE):
                source_filepath = os.path.join(root, filename)
                destination_filepath = os.path.join(DEST_INPUT_DIR, filename)

                try:
                    # ファイルをコピー
                    shutil.copy2(source_filepath, destination_filepath)
                    copied_files_count += 1
                    print(f"  ✅ コピーしました: {filename}")
                except Exception as e:
                    print(f"  ❌ エラー: {filename} のコピー中に問題が発生しました。エラー: {e}")
                    import traceback
                    traceback.print_exc()

    print(f"\n--- 買掛金CSVファイル抽出・コピー処理完了 ({datetime.now()}) ---")
    print(f"🎉 コピーされたファイル数: {copied_files_count} 🎉")

# --- メイン処理 ---
if __name__ == "__main__":
    filter_and_copy_csv_files()
    print(f"\n🎉 全ての抽出・コピー処理が完了しました！ ({datetime.now()}) 🎉")
    