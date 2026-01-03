import streamlit as st
from data.queries import get_source_data

def source_datasets():
    st.title("Source Datasets")
    st.caption("Raw source data from API-Football, stored in the src schema")
    
    dataset_options = {
        "Teams": "src_teams",
        "Fixtures": "src_fixtures",
        "Standings": "src_standings",
        "Scorers": "src_scorers"
    }
    
    selected_dataset = st.selectbox(
        "Select Dataset",
        options=list(dataset_options.keys()),
        index=0
    )
    
    table_name = dataset_options[selected_dataset]
    
    st.divider()
    st.subheader(f"{selected_dataset} Dataset")
    st.caption(f"Source table: {table_name}")
    
    try:
        df = get_source_data(table_name)
        
        if not df.empty:
            st.markdown(f"**Total Records:** {len(df)} (showing first 1000)")
            st.dataframe(df, use_container_width=True, height=600)
            
            st.divider()
            st.subheader("Dataset Statistics")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", len(df))
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024:.2f} KB")
        else:
            st.info("No data available for this dataset.")
    except Exception as e:
        st.error(f"Error loading dataset: {str(e)}")

def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Select Page",
        ["Home", "League Overview", "Team Overview", "Source Datasets"],
        index=0
    )