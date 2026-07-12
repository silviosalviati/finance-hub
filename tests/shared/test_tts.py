from unittest.mock import patch

from src.shared.tools import tts


def test_resolve_voice_name_usa_voz_configurada_por_genero():
    config = {
        "FINANCE_AUDITOR_TTS_VOICE": "pt-BR-Chirp3-HD-Achernar",
        "FINANCE_AUDITOR_TTS_VOICE_MASCULINA": "pt-BR-Chirp3-HD-Orus",
        "FINANCE_AUDITOR_TTS_VOICE_FEMININA": "pt-BR-Chirp3-HD-Vindemiatrix",
    }
    with patch.object(tts, "get_runtime_config", side_effect=lambda key, default="": config.get(key, default)):
        assert tts._resolve_voice_name("masculina") == "pt-BR-Chirp3-HD-Orus"
        assert tts._resolve_voice_name("feminina") == "pt-BR-Chirp3-HD-Vindemiatrix"


def test_resolve_voice_name_cai_no_fallback_quando_genero_nao_configurado():
    config = {"FINANCE_AUDITOR_TTS_VOICE": "pt-BR-Chirp3-HD-Achernar"}
    with patch.object(tts, "get_runtime_config", side_effect=lambda key, default="": config.get(key, default)):
        assert tts._resolve_voice_name("masculina") == "pt-BR-Chirp3-HD-Achernar"
        assert tts._resolve_voice_name("feminina") == "pt-BR-Chirp3-HD-Achernar"


def test_resolve_voice_name_genero_invalido_cai_no_fallback():
    config = {"FINANCE_AUDITOR_TTS_VOICE": "pt-BR-Chirp3-HD-Achernar"}
    with patch.object(tts, "get_runtime_config", side_effect=lambda key, default="": config.get(key, default)):
        assert tts._resolve_voice_name("neutra") == "pt-BR-Chirp3-HD-Achernar"
        assert tts._resolve_voice_name(None) == "pt-BR-Chirp3-HD-Achernar"
