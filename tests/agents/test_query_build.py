from src.agents.query_build import QueryBuildAgent


def test_query_build_agent_metadata():
    agent = QueryBuildAgent()
    assert agent.agent_id == "query_build"
    assert agent.display_name == "Query Build"
