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

def get_db_connection():
    """获取数据库连接"""
    return sqlite3.connect(DB_PATH)

def check_db_status():
    """检查数据库状态"""
    try:
        if not os.path.exists(DB_PATH):
            return 0
        conn = get_db_connection()
        cursor = conn.cursor()
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='top10_holders'")
        if not cursor.fetchone():
            conn.close()
            return 0
        # 统计记录数
        cursor.execute("SELECT COUNT(*) FROM top10_holders")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        st.error(f"检查数据库失败：{e}")
        return 0

def init_db():
    """初始化数据库表"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS top10_holders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT,
            stock_name TEXT,
            holder_name TEXT,
            holder_rank INTEGER,
            update_time TEXT
        )''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_holder ON top10_holders(holder_name)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_code ON top10_holders(stock_code)')
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"初始化数据库失败：{e}")
        return False

def update_data():
    """更新数据库"""
    progress_bar = st.progress(0)
    status_text = st.empty()
    count_text = st.empty()
    
    try:
        # 初始化数据库
        if not init_db():
            return False
        
        # 清空旧数据
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('DELETE FROM top10_holders')
        conn.commit()
        
        # 获取股票列表
        status_text.text("正在获取股票列表...")
        try:
            stock_df = ak.stock_info_a_code_name()
            stock_df = stock_df[stock_df['code'].str.startswith(('6', '0', '3'))]
        except Exception as e:
            status_text.text(f"获取股票列表失败：{e}")
            conn.close()
            return False
        
        total = len(stock_df)
        status_text.text(f"开始更新 {total} 只股票的数据...")
        
        batch_data = []
        inserted_count = 0
        error_count = 0
        
        for i, row in stock_df.iterrows():
            code = row['code']
            name = row['name']
            
            try:
                df = ak.stock_floatholder_top10(symbol=code)
                if df is not None and not df.empty and '股东名称' in df.columns:
                    holders = df['股东名称'].tolist()
                    update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    for rank, holder in enumerate(holders, 1):
                        if isinstance(holder, str) and holder.strip():
                            batch_data.append((
                                code,
                                name,
                                holder.strip(),
                                rank,
                                update_time
                            ))
                            inserted_count += 1
                    
                    # 每 100 条批量插入
                    if len(batch_data) >= 100:
                        c.executemany('''
                            INSERT INTO top10_holders 
                            (stock_code, stock_name, holder_name, holder_rank, update_time)
                            VALUES (?, ?, ?, ?, ?)
                        ''', batch_data)
                        conn.commit()
                        batch_data = []
                        
            except Exception as e:
                error_count += 1
                pass  # 跳过失败的股票
            
            # 更新进度
            progress_bar.progress((i + 1) / total)
            if i % 100 == 0:
                status_text.text(f"正在更新：{i+1}/{total} ({name})")
                count_text.info(f"✅ 已插入 {inserted_count:,} 条记录 | 失败 {error_count} 只股票")
            time.sleep(0.02)
        
        # 插入剩余数据
        if batch_data:
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
        count_text.empty()
        
        if final_count > 0:
            st.success(f"✅ 更新完成！共收录 {final_count:,} 条股东记录")
            st.info(f"📊 处理了 {total} 只股票，失败 {error_count} 只")
            return True
        else:
            st.error("❌ 更新完成，但数据库中无数据")
            return False
        
    except Exception as e:
        status_text.text(f"❌ 更新失败：{str(e)}")
        return False

def search_data(keywords):
    """搜索股东数据"""
    try:
        if not os.path.exists(DB_PATH):
            return pd.DataFrame()
        
        conn = get_db_connection()
        conditions = []
        params = []
        for kw in keywords:
            conditions.append("holder_name LIKE ?")
            params.append(f"%{kw}%")
        
        sql = f'''
            SELECT DISTINCT stock_code, stock_name, holder_name 
            FROM top10_holders 
            WHERE {" OR ".join(conditions)}
            ORDER BY stock_code
        '''
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"搜索失败：{e}")
        return pd.DataFrame()

# ========== 主界面 ==========
st.title("🗄️ A 股股东检索系统")
st.markdown("免安装 · 打开即用 · 支持多股东匹配")

# 初始化数据库
init_db()

# 检查数据库状态
count = check_db_status()

# ========== 侧边栏 ==========
with st.sidebar:
    st.header("📊 数据库状态")
    
    if count > 0:
        st.success(f"✅ 已收录 {count:,} 条记录")
        st.caption(f"更新时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    else:
        st.warning("⚠️ 数据库为空")
        st.info("👇 请点击下方按钮更新数据")
    
    st.markdown("---")
    st.header("🔄 数据管理")
    
    if st.button("📥 更新/重新加载数据", use_container_width=True, type="primary"):
        with st.spinner("⏱️ 正在更新数据，约需 20-30 分钟..."):
            if update_data():
                st.success("✅ 更新成功！")
                st.balloons()
                time.sleep(2)
                st.rerun()
            else:
                st.error("❌ 更新失败")
    
    # 数据下载功能
    if count > 0 and os.path.exists(DB_PATH):
        st.markdown("---")
        st.header("💾 数据备份")
        
        try:
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
        except Exception as e:
            st.error(f"下载失败：{e}")

# ========== 主区域 ==========
st.markdown("### 🔍 股东检索")

if count == 0:
    st.warning("⚠️ 数据库为空，请先在**左侧侧边栏**点击'更新数据'按钮")
    st.info("💡 首次使用需要下载全市场数据，约需 20-30 分钟")
    
    st.markdown("#### 使用示例：")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info("中央汇金")
    with col2:
        st.info("中国证券金融")
    with col3:
        st.info("高毅资产")
else:
    # 显示搜索界面
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
                lambda x: ' | '.join(sorted(set(x)))
            ).reset_index()
            result['match_count'] = df.groupby(['stock_code', 'stock_name']).size().values
            result = result.sort_values('match_count', ascending=False)
            
            st.success(f"✅ 找到 {len(result)} 只匹配股票")
            st.dataframe(result, use_container_width=True, hide_index=True)
            
            # 导出按钮
            csv = result.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 导出 CSV 文件", 
                data=csv.encode('utf-8-sig'),
                file_name=f"股东检索结果_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", 
                mime="text/csv",
                use_container_width=True
            )

# 页脚
st.markdown("---")
st.caption("💡 数据来源：AKShare | 仅供学习研究使用")
