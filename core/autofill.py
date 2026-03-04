"""
智能补全与检索回填引擎 (Auto-Fill & Verification Module)
掌管了功能 [2]：利用之前入库的记忆，给那些只有一半源系统数据，却空着 SAP 字段的 Excel 行进行“填空”。
也是最核心的调度器：精确匹配 A -> 模糊查找 B -> 千问大模型兜底 C 的总司令部！
"""
import os
import glob
import openpyxl
import chromadb
from datetime import datetime
from core.config import get_logger, get_script_dir
from core.excel_utils import find_headers, map_columns
from core.llm_service import evaluate_mapping_via_llm

logger = get_logger(__name__)

def auto_fill_excel(file_path: str, sheet_name: str, db_path: str, collection_name: str):
    """读取一份带有空缺目标字段的 Excel 模板，利用多级漏斗去寻找答案并写回新文件。"""
    logger.info(f"开始为您指定的残缺表单进行智能填补: {file_path}")
    
    # 尝试连上记忆库
    client = chromadb.PersistentClient(path=db_path)
    collection = None
    try:
        # 尝试抽出那本叫做 collection_name 的大词典
        collection = client.get_collection(name=collection_name)
    except Exception as e:
        # 万一以前根本没选过 [1] 学习功能，直接抛个错。不要紧，它照旧会坚强地运行大模型C兜底功能。
        logger.warning(f"目前记忆库里没有内容 或 库没建好. 将全盘依赖阿里的千问大模型发威. 具体原因: {e}")

    # 开箱 Excel
    wb = openpyxl.load_workbook(file_path)
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
    else:
        ws = wb.active

    # 找坐标，拿表头，定位源系统名称
    moto_cell, saki_cell = find_headers(ws)
    if not moto_cell or not saki_cell:
        logger.error("找不到模板对齐锚点。")
        return
        
    source_sys_name_cell = ws.cell(row=moto_cell.row + 1, column=moto_cell.column)
    source_sys_name = str(source_sys_name_cell.value).strip() if source_sys_name_cell.value else "未知源系统"

    header_row, col_src_desc, col_src_field, col_src_table, col_sap_desc, col_sap_table, col_sap_field = map_columns(ws, moto_cell, saki_cell)

    if None in (col_src_desc, col_src_field, col_sap_desc, col_sap_table, col_sap_field):
        logger.error("模版中残缺部分栏位")
        return

    # ======= 创建并定位：匹配来源 (A/B/C) 的标记列 =======
    col_match_source = None
    for cell in ws[header_row]:
        if cell.value == "マッチソース(A/B/C)":
            col_match_source = cell.column
            break
            
    if not col_match_source:
        col_match_source = ws.max_column + 1
        ws.cell(row=header_row, column=col_match_source).value = "マッチソース(A/B/C)"
    # ====================================================

    filled_count = 0
    # 扫每一行
    for row_idx in range(header_row + 1, ws.max_row + 1):
        src_field = str(ws.cell(row=row_idx, column=col_src_field).value or "").strip()
        src_desc = str(ws.cell(row=row_idx, column=col_src_desc).value or "").strip()
        src_table = str(ws.cell(row=row_idx, column=col_src_table).value or "").strip() if col_src_table else ""
        
        sap_table = ws.cell(row=row_idx, column=col_sap_table).value
        sap_field = ws.cell(row=row_idx, column=col_sap_field).value
        
        # 激活条件：只有左边有内容，必须且右边 SAP 是“空白”的，工具才会干活
        if src_field and src_desc and (not sap_table or not sap_field):
            hit_db = False  # 一个开关：用来标记“这行到底需不需要交去给最终的大模型”
            
            try:
                doc_text = f"[source_system:{source_sys_name}] [source_table:{src_table}] [source_field:{src_field}] [source_description:{src_desc}]"
                
                # ===============================================================
                # 漏斗第一关：【A 级查询】精准匹配 (也就是 Metadata 的 Where 条件硬对齐)
                # 只有曾经出现过一模一样的值，才会命中这里。
                # ===============================================================
                results = collection.query(
                    query_texts=[doc_text],
                    where={"$and": [
                        {"source_field_name": {"$eq": src_field}},
                        {"source_system_name": {"$eq": source_sys_name}}
                    ]},
                    n_results=1
                )
                
                # 命中了 A 级查询
                if results['ids'] and results['ids'][0]:
                    top_match = results['metadatas'][0][0]
                    # 直接在 Excel 当前行里盖戳！
                    ws.cell(row=row_idx, column=col_sap_table).value = top_match['sap_table_name']
                    ws.cell(row=row_idx, column=col_sap_field).value = top_match['sap_field_name']
                    if col_sap_desc:
                         ws.cell(row=row_idx, column=col_sap_desc).value = top_match.get('sap_field_desc', '')
                    ws.cell(row=row_idx, column=col_match_source).value = 'A' # 专属铭牌 A
                    
                    filled_count += 1
                    hit_db = True  # 打上此标记，说明已经被本系统内部数据库消化，不用拿去劳驾外网的大模型了
                    logger.info(f"Exact Match (级别 A) 命中: {src_field} -> {top_match.get('sap_table_name')}.{top_match.get('sap_field_name')}")
                else:
                    # ===============================================================
                    # 漏斗第二关：【B 级查询】语义/维度坐标模糊相似度匹配 (Semantic/Similarity match)
                    # 丢掉那些苛刻的 Where 约束条件，全靠文字语境去猜！
                    # ===============================================================
                    v_results = collection.query(
                        query_texts=[doc_text],
                        n_results=1
                    )
                    
                    if v_results['ids'] and v_results['ids'][0]:
                        distance = v_results['distances'][0][0] # 这就是刚才困扰过您的罪魁祸首：“数学向量余弦距离”
                        
                        # 这个 0.10 非常重要！！非常重要！！非常重要！！
                        # 超过 0.10 就意味着是“东施效颦”，比如“物料”撞上“移动类型”会有0.12，我们要把这种劣质匹配拒绝掉。
                        if distance < 0.10: 
                            v_match = v_results['metadatas'][0][0]
                            ws.cell(row=row_idx, column=col_sap_table).value = v_match['sap_table_name']
                            ws.cell(row=row_idx, column=col_sap_field).value = v_match['sap_field_name']
                            if col_sap_desc:
                                 ws.cell(row=row_idx, column=col_sap_desc).value = v_match.get('sap_field_desc', '')
                            ws.cell(row=row_idx, column=col_match_source).value = 'B' # 专属铭牌 B
                            filled_count += 1
                            hit_db = True
                            logger.info(f"Vector Match (级别 B) 命中: {src_field} -> {v_match.get('sap_table_name')}.{v_match.get('sap_field_name')} (误差距离: {distance:.3f})")
                        else:
                            # 就像刚才的 MATNR：距离虽然接近但不达标，果断一脚把它踹去下面的第三关
                            logger.info(f"库里搜到了近似词, 但误差 {distance:.3f} >= 0.10 未达标，直接把 '{src_field}' 丢给千问大模型.")
            except Exception as e:
                logger.warning(f"发生故障放弃查询 '{src_field}': {e}")
            
            # ===============================================================
            # 漏斗第三关：【C 级查询】Qwen 大脑线上推演 (LLM Request)
            # 全村的希望。当前两关都是白卷（打脸 False），进入这里。
            # ===============================================================
            if not hit_db:
                pred = evaluate_mapping_via_llm(src_field, src_desc)
                if pred and (pred.get("sap_table_name") or pred.get("sap_field_name")):
                    ws.cell(row=row_idx, column=col_sap_table).value = pred.get('sap_table_name', '')
                    ws.cell(row=row_idx, column=col_sap_field).value = pred.get('sap_field_name', '')
                    if col_sap_desc:
                         ws.cell(row=row_idx, column=col_sap_desc).value = pred.get('sap_field_desc', '')
                    ws.cell(row=row_idx, column=col_match_source).value = 'C' # 专属铭牌 C，说明这个填进去的答案是外星人(大模型)凭空捏的！
                    filled_count += 1
                    logger.info(f"Qwen 大模型 (级别 C) 推断回填: {src_field} -> {pred.get('sap_table_name')}.{pred.get('sap_field_name')}")
                else:
                    logger.info(f"Qwen 大模型也编造不出 '{src_field}'，留空被跳过！")
    
    # 全部填写完毕！输出保存。
    if filled_count > 0:
        script_dir = get_script_dir()
        autofilled_folder = os.path.join(script_dir, "autofilled_output")
        os.makedirs(autofilled_folder, exist_ok=True)
        
        base, ext = os.path.splitext(os.path.basename(file_path))
        # 文件名打上当前年月日分秒的签，防止您把原模板被覆盖污染！非常安全。
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_filename = os.path.join(autofilled_folder, f"{base}_autofilled_{timestamp}{ext}")
        wb.save(new_filename)
        logger.info(f"大成功！成功用魔法填补了 {filled_count} 行！全新文件已安全落地: {new_filename}")
    else:
        logger.info("没有任何行被填上。（可能是没空缺可以填，或者三个级别全部战败没有搜到对应选项）。")

def process_autofill(path: str, sheet_name: str, db_path: str, collection_name: str):
    """一个小型路由器：用来决定用户丢进来的是一个文件字典文件夹结构，还是单独一个文件，然后依次下发去运行上方的 auto_fill。"""
    if os.path.isdir(path):
        excel_files = glob.glob(os.path.join(path, "*.xlsx"))
        for file in excel_files:
            if not os.path.basename(file).startswith("~") and not "_autofilled" in file:
                auto_fill_excel(file, sheet_name, db_path, collection_name)
    else:
        auto_fill_excel(path, sheet_name, db_path, collection_name)

def process_update_and_autofill(path: str, sheet_name: str, db_path: str, collection_name: str):
    """
    功能 [3] 综合模式：
    分两阶段运行。
    第一段：扫描所有表结构里，那些有SAP目标映射值的，先扒出来吃进肚子里背掉（学习ingest）；
    第二段：转身去把刚才同一批表中空着一半没SAP结果的行，调用刚刚入库背好的新鲜记忆，以及大模型尝试再填上(自动生成)。
    """
    from core.ingestion import process_ingest
    if os.path.isdir(path):
        excel_files = glob.glob(os.path.join(path, "*.xlsx"))
        # Phase 1: Ingest all available knowledge first (知识沉淀)
        for file in excel_files:
            if not os.path.basename(file).startswith("~") and not "_autofilled" in file:
                process_ingest(file, sheet_name, db_path, collection_name)
        # Phase 2: Backfill any gaps using the newly enriched DB (大反攻：回填空缺)
        for file in excel_files:
            if not os.path.basename(file).startswith("~") and not "_autofilled" in file:
                auto_fill_excel(file, sheet_name, db_path, collection_name)
    else:
        # Phase 1
        process_ingest(path, sheet_name, db_path, collection_name)
        # Phase 2
        auto_fill_excel(path, sheet_name, db_path, collection_name)
