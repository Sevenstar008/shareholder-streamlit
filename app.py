import streamlit as st
import akshare as ak
import pandas as pd
import sqlite3
import os
import time
from datetime import datetime

st.set_page_config(page_title="A 股股东检索系统", layout="wide")

DB_FOLDER = "data"
DB_PATH = os.path.join(DB_FOLDER, "shareholders.db")
os.makedirs(DB_FOLDER, exist_ok=True)

# 检查数据库是否存在且有数据
def check_db_status():
    if not os.path.exists(DB_PATH):
        return 0
    try:
        conn = sqlite3.connect(DB_PATH)
        count = pd.read_sql_query("SELECT count(*) as c FROM top10_holders", conn)['c'][0]
        conn.close()
        return count
    except:
        return 0

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

def update_data():
    progress_bar = st.progress(0)
    status_text = st.empty()
    success_count = st.empty()
    
    try:
        # 初始化数据库
        init_db()
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # 清空旧数据
        c.execute('DELETE FROM top10_holders')
        conn.commit()
        
        # 获取股票列表
        status_text.text("正在获取股票列表...")
        stock_df = ak.stock_info_a_code_name()
        stock_df = stock_df[stock_df['code'].str.startswith(('6', '0', '3'))]
        total = len(stock_df)
        
        batch_data = []
        inserted_count = 0
        
        for i, row in stock_df.iterrows():
            code, name = row['code'], row['name']
            try:
                df = ak.stock_floatholder_top10(symbol=code)
                if df is not None and not df.empty and '股东名称' in df.columns:
                    holders = df['股东名称'].tolist()
                    for rank, holder in enumerate(holders, 1):
                        if isinstance(holder, str) and holder.strip():
                            batch_data.append((
                                code, 
                                name, 
                                holder.strip(), 
                                rank, 
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            ))
                            inserted_count += 1
                    
                    # 每 100 条批量插入一次
                    if len(batch_data) >= 100:
                        c.executemany('''
                            INSERT INTO top10_holders 
                            (stock_code, stock_name, holder_name, holder_rank, update_time) 
                            VALUES (?, ?, ?, ?, ?)
                        ''', batch_data)
                        conn.commit()
                        batch_data = []
                        
            except Exception as e:
                pass  # 跳过失败的股票
            
            # 更新进度
            progress_bar.progress((i + 1) / total)
            if i % 50 == 0:
                status_text.text(f"正在更新：{i+1}/{total} ({name})")
                success_count.info(f"✅ 已插入 {inserted_count:,} 条股东记录")
            time.sleep(0.02)  # 加快速度
        
        # 插入剩余数据
        if batch_
            c.executemany('''
                INSERT INTO top10_holders 
                (stock_code, stock_name, holder_name, holder_rank, update_time) 
                VALUES (?, ?, ?, ?, ?)
            ''', batch_data)
            conn.commit()
        
        conn.close()
        
        # 验证结果
        final_count = check_db_status()
        
        status_text.empty()
        progress_bar.empty()
        success_count.empty()
        
        st.success(f"✅ 更新完成！共收录 {final_count:,} 条股东记录")
        st.info(f"📊 处理了 {total} 只股票")
        
        return True
        
    except Exception as e:
        status_text.text(f"❌ 更新失败：{str(e)}")
        return False

def search_data(keywords):
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    
    try:
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
    except:
        return pd.DataFrame()

# ========== 主界面 ==========
st.title("🗄️ A 股股东检索系统")
st.markdown("免安装 · 打开即用 · 支持多股东匹配")

# 初始化数据库
init_db()

# 实时检查数据库状态
count = check_db_status()

# ========== 侧边栏 ==========
with st.sidebar:
    st.header("📊 数据库状态")
    
    if count > 0:
        st.success(f"✅ 已收录 {count:,} 条记录")
        st.caption(f"最后检查：{datetime.now().strftime('%H:%M:%S')}")
    else:
        st.warning("⚠️ 数据库为空")
        st.info("👇 请点击下方按钮更新数据")
    
    st.markdown("---")
    
    st.header("🔄 数据管理")
    
    if st.button("📥 更新/重新加载数据", use_container_width=True, type="primary"):
        st.info("⏱️ 首次更新约需 20-30 分钟，请耐心等待...")
        if update_data():
            st.balloons()
            time.sleep(2)
            st.rerun()
    
    # 下载数据库按钮（如果有数据）
    if count > 0 and os.path.exists(DB_PATH):
        st.markdown("---")
        st.header("💾 数据备份")
        
        with open(DB_PATH, 'rb') as f:
            db_bytes = f.read()
        
        st.download_button(
            label="📥 下载数据库文件",
            data=db_bytes,
            file_name=f"shareholders_{datetime.now().strftime('%Y%m%d_%H%M')}.db",
            mime="application/x-sqlite3",
            use_container_width=True
        )
        st.caption("下载后可本地保存备份")

# ========== 主区域 ==========
st.markdown("### 🔍 股东检索")

if count == 0:
    st.warning("⚠️ 数据库为空，请先在**左侧侧边栏**点击"更新数据"按钮")
    st.info("💡 首次使用需要下载全市场数据，约需 20-30 分钟")
    
    # 显示示例
    st.markdown("#### 使用示例：")
    st.code("输入：中央汇金，中国证券金融", language="text")
else:
    # 搜索输入框
    keywords = st.text_input(
        "输入股东名字（多个用逗号分隔）", 
        placeholder="例：中央汇金，中国证券金融，高毅资产",
        help="支持模糊匹配，输入关键词即可"
    )
    
    col1, col2 = st.columns([1, 5])
    with col1:
        search_btn = st.button("🔍 搜索", type="primary", use_container_width=True)
    
    if search_btn and keywords.strip():
        kw_list = [k.strip() for k in keywords.split(',') if k.strip()]
        with st.spinner('正在搜索...'):
            df = search_data(kw_list)
        
        if df.empty:
            st.info(f"🔍 未找到包含 '{', '.join(kw_list)}' 的股票")
        else:
            # 聚合显示
            result = df.groupby(['stock_code', 'stock_name'])['holder_name'].apply(
                lambda x: ' | '.join(x)
            ).reset_index()
            result['match_count'] = df.groupby(['stock_code', 'stock_name']).size().values
            result = result.sort_values('match_count', ascending=False)
            
            st.success(f"✅ 找到 {len(result)} 只匹配股票")
            st.dataframe(result, use_container_width=True, hide_index=True)
            
            # 导出按钮
            csv = result.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "📥 导出 CSV 文件", 
                csv, 
                f"股东检索结果_{datetime.now().strftime('%Y%m%d')}.csv", 
                "text/csv",
                use_container_width=True
            )

# 页脚
st.markdown("---")
st.caption("💡 数据来源：AKShare | 仅供学习研究使用")
