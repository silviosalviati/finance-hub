from src.agents.document_build import DocumentBuildAgent


def test_document_build_placeholder_response():
    agent = DocumentBuildAgent()
    result = agent.analyze("SELECT 1", "proj")

    assert result["status"] == "not_implemented"
