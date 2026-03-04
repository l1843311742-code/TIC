"""
全局配置与常量管理模块 (Configuration Module)
所有的基本设置（打印日志等级、需要固定的名字、所有的小脚本该去哪找数据库）都在这里统一管理。
如果以后要改 Sheet 名，只需要改这里一处，全项目生效！
"""
import os
import logging

# 配置日志在黑框里的打印样式： [INFO: 你好世界] 的格式
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_logger(module_name: str) -> logging.Logger:
    """提供一个标准的日志打印器，给项目其他模块统一使用"""
    return logging.getLogger(module_name)

# ========== 全局业务常量 ==========
# 您需要处理的 Excel 中被规定死的目标工作表名字
EXCEL_SHEET_NAME = "項目マッピング"
# 这个系统存放在 ChromaDB 单独的集合池名字
VECTOR_COLLECTION_NAME = "mapping_collection"

def get_script_dir() -> str:
    """
    寻找我们外层的 `TIC` 根目录的绝对路径！
    因为这个 config.py 在 `core/` 文件夹里，所以我们要向上一级找 (dirname)。
    这样就不会跑偏存到 C盘 或者用户文档里。
    """
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_db_path() -> str:
    """
    拼接出专门存 ChromaDB 那些高级浮点数向量文件的位置： `TIC/vector_store/`
    """
    return os.path.join(get_script_dir(), "vector_store")
