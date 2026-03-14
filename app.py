import streamlit as st
import akshare as ak
import pandas as pd
import sqlite3
import os
import time
from datetime import datetime

# 页面配置
st.set_page_config(page_title="A 股股东检索系统", layout="wide")

# 路径配置
DB_FOLDER = "data"
DB_PATH = os.path.join(DB_FOLDER, "shareholders.db")
os.makedirs(DB_FOLDER, exist_ok=True)

# 初始化数据库
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS top10_holders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code TEXT, stock_name TEXT, holder_name TEXT, 
        holder_rank INTEGER, update_time TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_holder ON top10_holders(holder_name)')
    conn.commit()
    conn.close()

# 更新数据函数
def update_data():
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM top10_holders')
        conn.commit()
        
        stock_df = ak.stock_info_a_code_name()
        stock_df = stock_df[stock_df['code'].str.startswith(('6', '0', '3'))]
        total = len(stock_df)
        
        batch_data = []
        for i, row in stock_df.iterrows():
            code, name = row['code'], row['name']
            try:
                df = ak.stock_floatholder_top10(symbol=code)
                if df is not None and not df.empty and '股东名称' in df.columns:
                    for rank, holder in enumerate(df['股东名称'].tolist(), 1):
                        if isinstance(holder, str):
                            batch_data.append((code, name, holder, rank, datetime.now().strftime('%Y-%m-%d')))
                
                if len(batch_data) >= 50:
                    c.executemany('INSERT INTO top10_holders VALUES (?,?,?,?,?)', batch_data)
                    conn.commit()
                    batch_data = []
            except:
                pass
            
            progress_bar.progress((i + 1) / total)
            if i % 100 == 0:
                status_text.text(f"正在更新：{i+1}/{total} ({name})")
            time.sleep(0.05)
        
        if batch_
            c.executemany('INSERT INTO top10_holders VALUES (?,?,?,?,?)', batch_data)
            conn.commit()
            
        conn.close()
        status_text.text("✅ 更新完成！")
        return True
    except Exception as e:
        status_text.text(f"❌ 更新失败：{str(e)}")
        return False

# 搜索函数
def search_data(keywords):
    conn = sqlite3.connect(DB_PATH)
    conditions = []
    params = []
    for kw in keywords:
        conditions.append("holder_name LIKE ?")
        params.append(f"%{kw}%")
    
    sql = f'SELECT stock_code, stock_name, holder_name FROM top10_holders WHERE {" OR ".join(conditions)}'
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df

# 主界面
st.title("🗄️ A 股股东检索系统")
st.markdown("免安装 · 打开即用 · 支持多股东匹配")

# 初始化数据库
init_db()

# 检查数据库状态
conn = sqlite3.connect(DB_PATH)
count = pd.read_sql_query("SELECT count(*) as c FROM top10_holders", conn)['c'][0]
conn.close()

# 侧边栏 - 始终显示
st.sidebar.header("📊 数据库状态")
if count > 0:
    st.sidebar.success(f"✅ 已收录 {count:,} 条记录")
else:
    st.sidebar.warning("⚠️ 数据库为空")

st.sidebar.markdown("---")
st.sidebar.header("🔄 数据管理")
if st.sidebar.button("📥 更新/重新加载数据", use_container_width=True):
    with st.sidebar:
        update_data()
        st.success("更新完成！")
        time.sleep(1)
        st.rerun()

# 主区域 - 始终显示搜索框
st.markdown("### 🔍 股东检索")
keywords = st.text_input("输入股东名字（多个用逗号分隔）", 
                        placeholder="例：中央汇金，中国证券金融，高毅资产",
                        help="支持模糊匹配，输入关键词即可")

col1, col2 = st.columns([1, 5])
with col1:
    search_btn = st.button("🔍 搜索", type="primary", use_container_width=True)

if search_btn:
    if not keywords.strip():
        st.warning("⚠️ 请输入股东名字")
    else:
        kw_list = [k.strip() for k in keywords.split(',') if k.strip()]
        with st.spinner('正在搜索...'):
            df = search_data(kw_list)
        
        if df.empty:
            if count == 0:
                st.error("❌ 数据库为空，请先在左侧点击"更新数据"按钮")
            else:
                st.info(f"🔍 未找到包含 "{"、".join(kw_list)}" 的股票")
        else:
            # 聚合显示
            result = df.groupby(['stock_code', 'stock_name'])['holder_name'].apply(lambda x: ' | '.join(x)).reset_index()
            result['match_count'] = df.groupby(['stock_code', 'stock_name']).size().values
            result = result.sort_values('match_count', ascending=False)
            
            st.success(f"✅ 找到 {len(result)} 只匹配股票")
            st.dataframe(result, use_container_width=True, hide_index=True)
            
            # 导出按钮
            csv = result.to_csv(index=False).encode('utf-8-sig')
            st.download_button("📥 导出 CSV 文件", 
                             csv, 
                             f"股东检索结果_{datetime.now().strftime('%Y%m%d')}.csv", 
                             "text/csv",
                             use_container_width=True)

# 页脚
st.markdown("---")
st.markdown("💡 **使用提示**：首次使用需点击左侧"更新数据"（约20分钟），之后即可快速检索")
st.caption("数据来源：AKShare | 仅供学习研究使用")
