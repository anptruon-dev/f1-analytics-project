"""
F1 Driver Performance Analytics Dashboard
Interactive Streamlit dashboard for F1 data analysis
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import sys
import os

# Add parent directories to path to import analytics
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.data_processing.analytics import F1Analytics

# Page configuration
st.set_page_config(
    page_title="F1 Driver Performance Analytics",
    page_icon="🏁",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize analytics
@st.cache_data
def load_analytics():
    """Load analytics data with caching"""
    analytics = F1Analytics('data/f1_database.db')
    return analytics

def load_driver_performance():
    """Load driver performance data"""
    analytics = load_analytics()
    return analytics.get_driver_performance_metrics()

def load_constructor_performance():
    """Load constructor performance data"""
    analytics = load_analytics()
    return analytics.get_constructor_performance()

def load_championship_progression():
    """Load championship progression data"""
    analytics = load_analytics()
    return analytics.get_championship_progression()

def load_circuit_performance():
    """Load circuit performance data"""
    analytics = load_analytics()
    return analytics.get_circuit_performance()

# Main dashboard
def main():
    st.title("🏁 Formula 1 Driver Performance Analytics")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.title("Dashboard Navigation")
    page = st.sidebar.selectbox(
        "Choose Analysis",
        ["Overview", "Driver Performance", "Constructor Analysis", "Championship Progression", "Circuit Analysis", "Head-to-Head Comparison"]
    )
    
    if page == "Overview":
        show_overview()
    elif page == "Driver Performance":
        show_driver_performance()
    elif page == "Constructor Analysis":
        show_constructor_analysis()
    elif page == "Championship Progression":
        show_championship_progression()
    elif page == "Circuit Analysis":
        show_circuit_analysis()
    elif page == "Head-to-Head Comparison":
        show_head_to_head()

def show_overview():
    """Display overview dashboard"""
    st.header("📊 Season Overview")
    
    # Load data
    driver_df = load_driver_performance()
    constructor_df = load_constructor_performance()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Drivers", len(driver_df))
    
    with col2:
        st.metric("Total Constructors", len(constructor_df))
    
    with col3:
        total_races = driver_df['races_completed'].iloc[0] if not driver_df.empty else 0
        st.metric("Races Completed", total_races)
    
    with col4:
        total_wins = driver_df['wins'].sum()
        st.metric("Total Wins", total_wins)
    
    # Championship standings
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Driver Championship")
        
        fig = px.bar(
            driver_df.head(10),
            x='driver_name',
            y='total_points',
            color='total_points',
            title="Top 10 Drivers by Points"
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🏭 Constructor Championship")
        
        fig = px.bar(
            constructor_df.head(10),
            x='constructor_name',
            y='total_points',
            color='total_points',
            title="Constructor Standings"
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

def show_driver_performance():
    """Display driver performance analysis"""
    st.header("🏎️ Driver Performance Analysis")
    
    driver_df = load_driver_performance()
    
    # Driver selector
    selected_drivers = st.multiselect(
        "Select Drivers to Compare",
        driver_df['driver_name'].tolist(),
        default=driver_df['driver_name'].head(5).tolist()
    )
    
    if selected_drivers:
        filtered_df = driver_df[driver_df['driver_name'].isin(selected_drivers)]
        
        # Performance metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Points vs Average Position")
            fig = px.scatter(
                filtered_df,
                x='avg_position',
                y='total_points',
                size='races_completed',
                hover_data=['driver_name', 'podiums', 'wins'],
                title="Driver Performance Scatter"
            )
            fig.update_xaxes(autorange="reversed")  # Lower position is better
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Podium Rate vs Win Rate")
            fig = px.scatter(
                filtered_df,
                x='podium_rate',
                y='win_rate',
                size='total_points',
                hover_data=['driver_name', 'races_completed'],
                title="Success Rate Analysis"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Performance consistency
        st.subheader("Performance Consistency")
        fig = px.bar(
            filtered_df.sort_values('position_consistency'),
            x='driver_name',
            y='position_consistency',
            title="Position Consistency (Lower is Better)",
            color='position_consistency',
            color_continuous_scale='RdYlGn_r'
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed metrics table
        st.subheader("Detailed Performance Metrics")
        display_columns = [
            'driver_name', 'constructor', 'total_points', 'avg_position', 
            'wins', 'podiums', 'podium_rate', 'win_rate', 'position_consistency'
        ]
        st.dataframe(
            filtered_df[display_columns].round(2),
            use_container_width=True
        )

def show_constructor_analysis():
    """Display constructor analysis"""
    st.header("🏭 Constructor Performance Analysis")
    
    constructor_df = load_constructor_performance()
    
    # Constructor metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Constructor Points Distribution")
        fig = px.pie(
            constructor_df,
            values='total_points',
            names='constructor_name',
            title="Points Share by Constructor"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Average Position by Constructor")
        fig = px.bar(
            constructor_df.sort_values('avg_position'),
            x='constructor_name',
            y='avg_position',
            title="Average Finishing Position",
            color='avg_position',
            color_continuous_scale='RdYlGn_r'
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    # Performance comparison
    st.subheader("Constructor Performance Matrix")
    fig = px.scatter(
        constructor_df,
        x='avg_position',
        y='total_points',
        size='podiums',
        hover_data=['constructor_name', 'wins', 'podium_rate'],
        title="Constructor Performance Overview"
    )
    fig.update_xaxes(autorange="reversed")
    st.plotly_chart(fig, use_container_width=True)
    
    # Constructor table
    st.subheader("Constructor Standings Table")
    display_columns = [
        'constructor_name', 'nationality', 'total_points', 'avg_position',
        'wins', 'podiums', 'podium_rate', 'win_rate'
    ]
    st.dataframe(
        constructor_df[display_columns].round(2),
        use_container_width=True
    )

def show_championship_progression():
    """Display championship progression"""
    st.header("🏆 Championship Progression")
    
    progression_df = load_championship_progression()
    
    if not progression_df.empty:
        # Top drivers selector
        top_drivers = progression_df.groupby('driver_name')['cumulative_points'].max().sort_values(ascending=False).head(8).index.tolist()
        
        selected_drivers = st.multiselect(
            "Select Drivers to Track",
            top_drivers,
            default=top_drivers[:5]
        )
        
        if selected_drivers:
            filtered_df = progression_df[progression_df['driver_name'].isin(selected_drivers)]
            
            # Championship progression line chart
            fig = px.line(
                filtered_df,
                x='round',
                y='cumulative_points',
                color='driver_name',
                title="Championship Points Progression",
                markers=True
            )
            fig.update_layout(
                xaxis_title="Race Round",
                yaxis_title="Cumulative Points"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Race-by-race points
            st.subheader("Race-by-Race Points")
            race_points_df = filtered_df.pivot(index='round', columns='driver_name', values='race_points').fillna(0)
            
            fig = px.bar(
                race_points_df.reset_index().melt(id_vars='round'),
                x='round',
                y='value',
                color='driver_name',
                title="Points Scored per Race",
                barmode='group'
            )
            st.plotly_chart(fig, use_container_width=True)

def show_circuit_analysis():
    """Display circuit analysis"""
    st.header("🏁 Circuit Performance Analysis")
    
    circuit_df = load_circuit_performance()
    
    if not circuit_df.empty:
        # Circuit selector
        selected_circuits = st.multiselect(
            "Select Circuits",
            circuit_df['circuit_name'].tolist(),
            default=circuit_df['circuit_name'].head(5).tolist()
        )
        
        if selected_circuits:
            filtered_df = circuit_df[circuit_df['circuit_name'].isin(selected_circuits)]
            
            # Circuit statistics
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Most Successful Drivers by Circuit")
                fig = px.bar(
                    filtered_df,
                    x='circuit_name',
                    y='driver_wins_at_circuit',
                    hover_data=['most_successful_driver'],
                    title="Driver Success at Circuits"
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Constructor Success by Circuit")
                fig = px.bar(
                    filtered_df,
                    x='circuit_name',
                    y='constructor_wins_at_circuit',
                    hover_data=['most_successful_constructor'],
                    title="Constructor Success at Circuits"
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            # Circuit details table
            st.subheader("Circuit Performance Details")
            display_columns = [
                'circuit_name', 'country', 'total_races', 'most_successful_driver',
                'driver_wins_at_circuit', 'most_successful_constructor', 'constructor_wins_at_circuit'
            ]
            st.dataframe(
                filtered_df[display_columns],
                use_container_width=True
            )

def show_head_to_head():
    """Display head-to-head comparison"""
    st.header("⚔️ Head-to-Head Driver Comparison")
    
    driver_df = load_driver_performance()
    analytics = load_analytics()
    
    # Driver selectors
    col1, col2 = st.columns(2)
    
    with col1:
        driver1 = st.selectbox("Select Driver 1", driver_df['driver_name'].tolist(), index=0)
        driver1_id = driver_df[driver_df['driver_name'] == driver1]['driver_id'].iloc[0]
    
    with col2:
        driver2 = st.selectbox("Select Driver 2", driver_df['driver_name'].tolist(), index=1)
        driver2_id = driver_df[driver_df['driver_name'] == driver2]['driver_id'].iloc[0]
    
    if driver1 != driver2:
        # Get head-to-head data
        h2h_df, h2h_stats = analytics.get_head_to_head_comparison(driver1_id, driver2_id)
        
        if h2h_stats and h2h_stats['total_races'] > 0:
            # H2H Statistics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Races", h2h_stats['total_races'])
            
            with col2:
                st.metric(f"{driver1} Wins", h2h_stats['driver1_wins'])
            
            with col3:
                st.metric(f"{driver2} Wins", h2h_stats['driver2_wins'])
            
            with col4:
                st.metric("Ties", h2h_stats['ties'])
            
            # Performance comparison
            comparison_data = {
                'Metric': ['Average Position', 'Total Points'],
                driver1: [h2h_stats['driver1_avg_position'], h2h_stats['driver1_total_points']],
                driver2: [h2h_stats['driver2_avg_position'], h2h_stats['driver2_total_points']]
            }
            
            comparison_df = pd.DataFrame(comparison_data)
            st.subheader("Performance Comparison")
            st.dataframe(comparison_df, use_container_width=True)
            
            # Race-by-race comparison
            if not h2h_df.empty:
                st.subheader("Race-by-Race Results")
                st.dataframe(h2h_df, use_container_width=True)
        else:
            st.warning("No head-to-head data found for these drivers.")

if __name__ == "__main__":
    main()