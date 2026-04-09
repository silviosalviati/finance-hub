from src.agents.query_analyzer import QueryAnalyzerAgent


def test_query_analyzer_agent_metadata():
    agent = QueryAnalyzerAgent()
    assert agent.agent_id == "query_analyzer"
    assert agent.display_name == "Query Analyzer"
