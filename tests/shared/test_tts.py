from unittest.mock import patch

from src.shared.tools import tts


def test_get_or_create_voice_preview_genero_invalido():
    result = tts.get_or_create_voice_preview("neutra")
    assert result["ok"] is False
    assert "inválido" in result["error"]


def test_get_or_create_voice_preview_reusa_cache_existente(tmp_path):
    cached = tmp_path / "preview_feminina.mp3"
    cached.write_bytes(b"fake-mp3")

    with (
        patch.object(tts, "_PODCAST_ROOT", tmp_path),
        patch.object(tts, "synthesize_ptbr_mp3") as fake_synth,
    ):
        result = tts.get_or_create_voice_preview("feminina")

    fake_synth.assert_not_called()
    assert result == {"ok": True, "mime_type": "audio/mpeg", "audio_path": str(cached)}


def test_get_or_create_voice_preview_gera_quando_nao_ha_cache(tmp_path):
    with (
        patch.object(tts, "_PODCAST_ROOT", tmp_path),
        patch.object(
            tts, "synthesize_ptbr_mp3", return_value={"ok": True, "mime_type": "audio/mpeg"}
        ) as fake_synth,
    ):
        result = tts.get_or_create_voice_preview("masculina")

    fake_synth.assert_called_once()
    assert fake_synth.call_args.args[0] == tts._PREVIEW_TEXT
    assert fake_synth.call_args.kwargs["asset_id"] == "preview_masculina"
    assert fake_synth.call_args.kwargs["gender"] == "masculina"
    assert result["ok"] is True


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
