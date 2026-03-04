"""
主程序入口文件 (Main Entry Point)
这个文件只负责：
1. 显示给用户的黑框框交互菜单 (CLI)
2. 接收用户输入的数字和路径
3. 把真正的核心工作交接给 `core/` 文件夹里对应的专门模块去执行
"""
import os
# 从我们自己写的核心库导入必要的常量和方法
from core.config import EXCEL_SHEET_NAME, VECTOR_COLLECTION_NAME, get_db_path
from core.ingestion import process_ingest
from core.autofill import process_autofill, process_update_and_autofill

def main():
    # 打印欢迎菜单
    print("==================================================")
    print("=== スマート Excel Mappings 処理センターへようこそ ===")
    print("必要な操作機能を選択してください（数字を直接入力）：")
    print("[1] データ解析とベクトルテーブルへの登録 (学習と蓄積)")
    print("[2] データ照会・自動マッピング・コピー保存 (欠落属性の自動補完)")
    # 这行目前隐去，如果以后需要可以放开
    # print("[3] 更新并回填 (综合模式：先解析当前表的所有已知属性存入库，再利用新知识填补空缺)")
    print("==================================================")
    
    # 获取用户输入的功能号，并且去掉多余的空格
    choice = input("機能番号を入力してください (1/2): ").strip()
    if choice not in ['1', '2']:
        print("無効な選択です。プログラムを終了します。")
        sys.exit(1)
        
    # 获取需要处理的文件或者文件夹路径
    path = input("処理するファイルまたはフォルダの絶対パスを入力してください: ").strip()
    
    # 清理路径：如果用户是从电脑拖拽文件进来，路径可能会带双引号或单引号，这步是为了把引号统统剥掉
    if path.startswith('"') and path.endswith('"'):
        path = path[1:-1]
    elif path.startswith("'") and path.endswith("'"):
        path = path[1:-1]

    # 获取全局配置常量（需要读哪个 Sheet 名，存在哪，叫什么集合）
    sheet_name = EXCEL_SHEET_NAME
    db_path = get_db_path()
    collection_name = VECTOR_COLLECTION_NAME

    # 根据用户的选择，分发给不同的小弟（core里的模块）去干活
    if choice == '1':
        print(f"\n---> [機能 1] データの解析とデータベース登録を開始します: {path}")
        # 调用 core/ingestion.py 里的函数去学习源数据并存入 ChromaDB
        process_ingest(path, sheet_name, db_path, collection_name)
    elif choice == '2':
        print(f"\n---> [機能 2] スマート自動補完を開始します: {path}")
        # 调用 core/autofill.py 里的函数去调用 ChromaDB(相似匹配) 甚至大模型 Qwen 来自动补全空缺
        process_autofill(path, sheet_name, db_path, collection_name)
    elif choice == '3':
        print(f"\n---> [機能 3] 統合モード (更新と自動補完) を開始します: {path}")
        # 先学习再补全
        process_update_and_autofill(path, sheet_name, db_path, collection_name)

# 确保只有在直接运行这个脚本的时候才会弹菜单（防止被别人 import 时误弹）
if __name__ == "__main__":
    main()