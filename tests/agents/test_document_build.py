from src.agents.document_build import DocumentBuildAgent


def test_document_build_agent_metadata():
    agent = DocumentBuildAgent()
    assert agent.agent_id == "document_build"
    assert agent.display_name == "Document Build"


def test_document_build_runtime_info():
    agent = DocumentBuildAgent()
    info = agent.runtime_info()

    assert info["provider"] == "shared"
