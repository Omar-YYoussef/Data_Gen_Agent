import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent.parent))

try:
    from config.settings import settings
    from utils.json_handler import JsonHandler
except ImportError as e:
    st.error(f"Import error: {e}")
    st.error("Please make sure you're running from the project root directory")
    st.stop()

st.set_page_config(
    page_title="Pipeline Monitoring Dashboard",
    page_icon="📊",
    layout="wide"
)

def load_workflow_logs():
    """Load all workflow logs for analysis"""
    try:
        workflow_logs = list(settings.WORKFLOW_LOGS_PATH.glob("workflow_*.json"))
        
        data = []
        for log_file in workflow_logs:
            workflow_state = JsonHandler.load_json(log_file)
            if workflow_state:
                data.append(workflow_state)
        
        return data
    except Exception as e:
        st.error(f"Error loading workflow logs: {e}")
        return []

def main():
    st.title("🔬 Web Search & Synthetic Data Pipeline Dashboard")
    
    # Load data
    workflows = load_workflow_logs()
    
    if not workflows:
        st.warning("No workflow data found")
        st.info("Make sure your pipeline has generated some workflow logs")
        return
    
    # Convert to DataFrame
    df_data = []
    for wf in workflows:
        try:
            df_data.append({
                'workflow_id': wf.get('workflow_id', 'Unknown'),
                'status': wf.get('status', 'Unknown'),
                'domain_type': wf.get('parsed_query', {}).get('domain_type', 'Unknown'),
                'data_type': wf.get('parsed_query', {}).get('data_type', 'Unknown'),
                'language': wf.get('parsed_query', {}).get('language', 'Unknown'),
                'sample_count': wf.get('parsed_query', {}).get('sample_count', 0),
                'timestamp': wf.get('timestamp', datetime.now().isoformat()),
                'total_topics': wf.get('final_metrics', {}).get('total_topics', 0),
                'delivered_samples': wf.get('final_metrics', {}).get('delivered_samples', 0)
            })
        except Exception as e:
            st.warning(f"Error processing workflow: {e}")
    
    if not df_data:
        st.error("No valid workflow data could be processed")
        return
    
    df = pd.DataFrame(df_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    status_filter = st.sidebar.multiselect(
        "Status", 
        options=df['status'].unique(),
        default=df['status'].unique()
    )
    
    domain_filter = st.sidebar.multiselect(
        "Domain",
        options=df['domain_type'].unique(),
        default=df['domain_type'].unique()
    )
    
    # Filter data
    filtered_df = df[
        (df['status'].isin(status_filter)) &
        (df['domain_type'].isin(domain_filter))
    ]
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Workflows", len(filtered_df))
    
    with col2:
        success_rate = (filtered_df['status'] == 'completed').mean() * 100 if len(filtered_df) > 0 else 0
        st.metric("Success Rate", f"{success_rate:.1f}%")
    
    with col3:
        total_samples = filtered_df['delivered_samples'].sum()
        st.metric("Total Samples Generated", f"{total_samples:,}")
    
    with col4:
        avg_topics = filtered_df['total_topics'].mean() if len(filtered_df) > 0 else 0
        st.metric("Avg Topics per Run", f"{avg_topics:.1f}")
    
    # Charts
    st.header("📈 Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Status distribution
        status_counts = filtered_df['status'].value_counts()
        if len(status_counts) > 0:
            fig_status = px.pie(
                values=status_counts.values,
                names=status_counts.index,
                title="Workflow Status Distribution"
            )
            st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.info("No data available for status distribution")
    
    with col2:
        # Domain distribution
        domain_counts = filtered_df['domain_type'].value_counts()
        if len(domain_counts) > 0:
            fig_domain = px.bar(
                x=domain_counts.index,
                y=domain_counts.values,
                title="Requests by Domain"
            )
            st.plotly_chart(fig_domain, use_container_width=True)
        else:
            st.info("No data available for domain distribution")
    
    # Time series
    st.header("🕒 Timeline Analysis")
    
    # Workflows over time
    if len(filtered_df) > 0:
        daily_counts = filtered_df.groupby(filtered_df['timestamp'].dt.date).size()
        
        fig_timeline = px.line(
            x=daily_counts.index,
            y=daily_counts.values,
            title="Workflows per Day",
            labels={'x': 'Date', 'y': 'Number of Workflows'}
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("No data available for timeline analysis")
    
    # Recent workflows table
    st.header("📋 Recent Workflows")
    
    if len(filtered_df) > 0:
        recent_df = filtered_df.sort_values('timestamp', ascending=False).head(10)
        
        st.dataframe(
            recent_df[['workflow_id', 'status', 'domain_type', 'data_type', 
                      'sample_count', 'delivered_samples', 'timestamp']],
            use_container_width=True
        )
    else:
        st.info("No workflows to display")
    
    # Detailed workflow analysis
    if len(filtered_df) > 0 and st.button("Show Detailed Analysis"):
        st.header("🔍 Detailed Analysis")
        
        # Performance by domain
        domain_performance = filtered_df.groupby('domain_type').agg({
            'delivered_samples': ['mean', 'sum'],
            'total_topics': 'mean',
            'status': lambda x: (x == 'completed').mean()
        }).round(2)
        
        st.subheader("Performance by Domain")
        st.dataframe(domain_performance)

if __name__ == "__main__":
    main()