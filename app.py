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

def check_db_status():
    """检查数据库状态"""
    try:
        if not os.path.exists(DB_PATH):
            return 0
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM top10_holders")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except:
        return 0

def init_db():
    """初始化数据库"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # 删除旧表重建
        c.execute('DROP TABLE IF EXISTS top10_holders')
        
        # 创建新表
        c.execute('''
            CREATE TABLE top10_holders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                holder_name TEXT NOT NULL,
                holder_rank INTEGER,
                update_time TEXT
            )
        ''')
        
        # 创建索引
        c.execute('CREATE INDEX idx_holder ON top10_holders(holder_name)')
        c.execute('CREATE INDEX idx_code ON top10_holders(stock_code)')
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"初始化数据库失败：{e}")
        return False

def update_data():
    """更新数据"""
    progress_bar = st.progress(0)
    status_text = st.empty()
    info_text = st.empty()
    
    try:
        # 初始化数据库
        status_text.text("正在初始化数据库...")
        if not init_db():
            return False
        
        # 获取股票列表
        status_text.text("正在获取股票列表...")
        try:
            stock_df = ak.stock_info_a_code_name()
            stock_df = stock_df[stock_df['code'].str.startswith(('6', '0', '3'))]
        except Exception as e:
            status_text.text(f"获取股票列表失败：{e}")
            return False
        
        total = len(stock_df)
        status_text.text(f"开始更新 {total} 只股票...")
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        success_count = 0
        error_count = 0
        total_records = 0
        
        for i, row in stock_df.iterrows():
            code = row['code']
            name = row['name']
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            try:
                # 获取十大流通股东
                df = ak.stock_floatholder_top10(symbol=code)
                
                if df is not None and not df.empty and '股东名称' in df.columns:
                    holders = df['股东名称'].tolist()
                    
                    # 逐条插入（更稳定）
                    for rank, holder in enumerate(holders, 1):
                        if isinstance(holder, str) and holder.strip():
                            try:
                                c.execute('''
                                    INSERT INTO top10_holders 
                                    (stock_code, stock_name, holder_name, holder_rank, update_time)
                                    VALUES (?, ?, ?, ?, ?)
                                ''', (code, name, holder.strip(), rank, update_time))
                                total_records += 1
                            except Exception as e:
                                error_count += 1
                    
                    success_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                error_count += 1
                pass
            
            # 每 100 只股票提交一次
            if (i + 1) % 100 == 0:
                conn.commit()
            
            # 更新进度
            progress_bar.progress((i + 1) / total)
            if i % 100 == 0:
                status_text.text(f"正在更新：{i+1}/{total} ({name})")
                info_text.info(f"✅ 成功 {success_count} 只 | 已插入 {total_records:,} 条记录 | 失败 {error_count} 只")
            
            time.sleep(0.05)
        
        # 最后提交
        conn.commit()
        conn.close()
        
        # 验证结果
        final_count = check_db_status()
        
        status_text.empty()
        progress_bar.empty()
        info_text.empty()
        
        if final_count > 0:
            st.success(f"✅ 更新完成！共收录 {final_count:,} 条股东记录")
            st.info(f"📊 成功 {success_count} 只股票 | 失败 {error_count} 只 | 总记录 {total_records:,} 条")
            return True
        else:
            st.error("❌ 更新完成，但数据库中无数据")
            return False
        
    except Exception as e:
        status_text.text(f"❌ 更新失败：{str(e)}")
        return False

def search_data(keywords):
    """搜索数据"""
    try:
        if not os.path.exists(DB_PATH):
            return pd.DataFrame()
        
        conn = sqlite3.connect(DB_PATH)
        conditions = []
        params = []
        for kw in keywords:
            conditions.append("holder_name LIKE ?")
            params.append(f"%{kw}%")
        
        sql = f'''
            SELECT DISTINCT stock_code, stock_name, holder_name 
            FROM top10_holders 
            WHERE {" OR ".join(conditions)}
        '''
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()
        return df
    except:
        return pd.DataFrame()

# ========== 主界面 ==========
st.title("🗄️ A 股股东检索系统")

# 初始化
init_db()
count = check_db_status()

# 侧边栏
with st.sidebar:
    st.header("📊 数据库状态")
    
    if count > 0:
        st.success(f"✅ 已收录 {count:,} 条记录")
    else:
        st.warning("⚠️ 数据库为空")
    
    st.markdown("---")
    st.header("🔄 数据管理")
    
    if st.button("📥 更新数据", use_container_width=True, type="primary"):
        if update_data():
            st.balloons()
            time.sleep(2)
            st.rerun()
    
    # 下载数据库
    if count > 0 and os.path.exists(DB_PATH):
        st.markdown("---")
        with open(DB_PATH, 'rb') as f:
            db_bytes = f.read()
        
        st.download_button(
            "📥 下载数据库",
            data=db_bytes,
            file_name=f"shareholders_{datetime.now().strftime('%Y%m%d')}.db",
            mime="application/x-sqlite3",
            use_container_width=True
        )

# 主区域
if count == 0:
    st.warning("⚠️ 数据库为空，请先点击左侧'更新数据'按钮")
else:
    st.markdown("### 🔍 股东检索")
    keywords = st.text_input("输入股东名字（逗号分隔）", placeholder="例：中央汇金，中国证券金融")
    
    if st.button("🔍 搜索", type="primary"):
        if keywords.strip():
            kw_list = [k.strip() for k in keywords.split(',') if k.strip()]
            df = search_data(kw_list)
            
            if not df.empty:
                result = df.groupby(['stock_code', 'stock_name'])['holder_name'].apply(
                    lambda x: ' | '.join(x)
                ).reset_index()
                st.dataframe(result, use_container_width=True)
                
                csv = result.to_csv(index=False).encode('utf-8-sig')
                st.download_button("📥 导出 CSV", csv, "result.csv", "text/csv")
            else:
                st.info("未找到匹配的股票")

st.caption("数据来源：AKShare")
