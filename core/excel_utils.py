"""
Excel 表格识别与寻址专属模块 (Excel Utilities)
专门用来定位：到底在哪一行是我们要的表头？源系统和目标 SAP 系统的数据到底在第几列？
"""

def find_headers(ws):
    """
    动态寻找表头坐标！
    它会在表格的前 20 行里“扫描”，只有当同时找到了包含【連携元】和【連携先】这两个大标题的行，
    才会把这两个格子的准确位置退回去，方便后面的程序顺藤摸瓜。
    """
    renkei_moto_cell = None
    renkei_saki_cell = None

    # 只在前 20 行找，节约性能
    for row in ws.iter_rows(min_row=1, max_row=20):
        for cell in row:
            # 清除空格以便能稳定识别
            val = str(cell.value).strip().replace(" ", "").replace("　", "") if cell.value is not None else ""
            if val == "連携元":
                renkei_moto_cell = cell
            elif val == "連携先":
                renkei_saki_cell = cell
            
            # 两个都同时凑齐了，咱们就可以收工跳出循环了
            if renkei_moto_cell and renkei_saki_cell:
                break
        if renkei_moto_cell and renkei_saki_cell:
            break

    return renkei_moto_cell, renkei_saki_cell

def map_columns(ws, renkei_moto_cell, renkei_saki_cell):
    """
    这个函数专门负责确定源字段和 SAP 目标映射各在 Excel 列 A 到 Z 里的具体第几列！
    """
    # 真正的属性小标题（构造、名称这些），是在“連携元”下面隔了 2 行的地方（所以是 +2）
    header_row = renkei_moto_cell.row + 2
    
    # 提前准备好空位，等下找到了就把列号填进来
    col_src_desc, col_src_field, col_src_table = None, None, None
    col_sap_desc, col_sap_table, col_sap_field = None, None, None

    moto_col = renkei_moto_cell.column
    saki_col = renkei_saki_cell.column

    # 循环我们找好的小标题行
    for cell in ws[header_row]:
        val = str(cell.value).strip().replace(" ", "").replace("　", "") if cell.value is not None else ""
        if not val:
            continue
            
        col_idx = cell.column  # 获取这是第几列 (比如 A=1, B=2)
        
        # 兼容左右位置互换的情况：判断当前列属于源系统区 (moto) 还是 目标系统区 (saki)
        if moto_col < saki_col:
            is_moto = moto_col <= col_idx < saki_col
            is_saki = col_idx >= saki_col
        else:
            is_saki = saki_col <= col_idx < moto_col
            is_moto = col_idx >= moto_col
            
        if is_moto:
            # 宽容匹配中文和日文，记录对应的列号
            if val == "项目名称" or val == "項目名称":
                col_src_desc = col_idx
            elif val == "技术名称" or val == "技術名称":
                col_src_field = col_idx
            elif val in ("构造", "構造", "テーブル", "表名"):
                col_src_table = col_idx
                
        elif is_saki:
            if val == "项目名称" or val == "項目名称":
                col_sap_desc = col_idx
            elif val == "构造" or val == "構造" or val in ("テーブル", "表名"):
                col_sap_table = col_idx
            elif val == "技术名称" or val == "技術名称":
                col_sap_field = col_idx
                
    # 把找了一圈最后确定的 7 个核心坐标打包送给外层的代码 (新增返回这两个 cell 锚点)
    return header_row, renkei_moto_cell, renkei_saki_cell, col_src_desc, col_src_field, col_src_table, col_sap_desc, col_sap_table, col_sap_field
