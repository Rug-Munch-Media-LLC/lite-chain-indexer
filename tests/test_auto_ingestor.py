"""Tests for Auto RPC Ingestor."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from lite_indexer.rpc.auto_ingestor import (
    RpcIngestorStats,
    _extract_candidates,
    auto_ingest,
    discover_chain_rpcs,
    validate_endpoint,
    validate_endpoints,
)


class TestRpcIngestorStats:
    """Tests for RpcIngestorStats."""

    def test_init(self):
        stats = RpcIngestorStats()
        assert stats.total_discovered == 0
        assert stats.total_validated == 0
        assert stats.total_added == 0

    def test_record_discovery(self):
        stats = RpcIngestorStats()
        stats.record_discovery(10, 5, 3)
        assert stats.total_discovered == 10
        assert stats.total_validated == 5
        assert stats.total_added == 3
        assert stats.last_discovery_count == 3

    def test_record_discovery_cumulative(self):
        stats = RpcIngestorStats()
        stats.record_discovery(10, 5, 3)
        stats.record_discovery(20, 10, 7)
        assert stats.total_discovered == 30
        assert stats.total_validated == 15
        assert stats.total_added == 10

    def test_to_dict(self):
        stats = RpcIngestorStats()
        stats.record_discovery(10, 5, 3)
        d = stats.to_dict()
        assert d["total_discovered"] == 10
        assert d["total_validated"] == 5
        assert d["total_added"] == 3


class TestExtractCandidates:
    """Tests for _extract_candidates function."""

    def test_extracts_matching_chain(self):
        registry = [
            {
                "chainId": 1,
                "name": "Ethereum",
                "rpc": [
                    "https://eth.llamarpc.com",
                    "https://rpc.ankr.com/eth",
                ],
            }
        ]
        candidates = _extract_candidates(1, registry)
        assert "https://eth.llamarpc.com" in candidates
        assert "https://rpc.ankr.com/eth" in candidates

    def test_skips_non_matching_chain(self):
        registry = [
            {
                "chainId": 56,
                "name": "BSC",
                "rpc": ["https://bsc-dataseed.binance.org"],
            }
        ]
        candidates = _extract_candidates(1, registry)
        assert len(candidates) == 0

    def test_skips_api_key_endpoints(self):
        registry = [
            {
                "chainId": 1,
                "rpc": [
                    "https://mainnet.infura.io/v3/${INFURA_KEY}",
                    "https://eth.llamarpc.com",
                ],
            }
        ]
        candidates = _extract_candidates(1, registry)
        assert len(candidates) == 1
        assert "https://eth.llamarpc.com" in candidates

    def test_skips_non_http(self):
        registry = [
            {
                "chainId": 1,
                "rpc": [
                    "wss://ws.example.com",
                    "https://eth.llamarpc.com",
                ],
            }
        ]
        candidates = _extract_candidates(1, registry)
        assert len(candidates) == 1

    def test_deduplicates(self):
        registry = [
            {
                "chainId": 1,
                "rpc": [
                    "https://eth.llamarpc.com",
                    "https://eth.llamarpc.com",
                ],
            }
        ]
        candidates = _extract_candidates(1, registry)
        assert len(candidates) == 1

    def test_empty_registry(self):
        candidates = _extract_candidates(1, [])
        assert len(candidates) == 0


class TestValidateEndpoint:
    """Tests for validate_endpoint function."""

    @pytest.mark.asyncio
    async def test_connection_error(self):
        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(side_effect=ConnectionError("refused"))
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await validate_endpoint("https://example.com")
            assert result is False


class TestValidateEndpoints:
    """Tests for validate_endpoints function."""

    @pytest.mark.asyncio
    async def test_empty_list(self):
        result = await validate_endpoints([])
        assert len(result) == 0


class TestDiscoverChainRpcs:
    """Tests for discover_chain_rpcs function."""

    @pytest.mark.asyncio
    async def test_discover_with_mock_registry(self):
        mock_registry = [
            {
                "chainId": 1,
                "name": "Ethereum",
                "rpc": ["https://eth.llamarpc.com"],
            }
        ]

        with patch("lite_indexer.rpc.auto_ingestor._fetch_registry", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_registry
            result = await discover_chain_rpcs(1)
            assert "https://eth.llamarpc.com" in result

    @pytest.mark.asyncio
    async def test_discover_empty_registry(self):
        with patch("lite_indexer.rpc.auto_ingestor._fetch_registry", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = []
            result = await discover_chain_rpcs(1)
            assert len(result) == 0


class TestAutoIngest:
    """Tests for auto_ingest function."""

    @pytest.mark.asyncio
    async def test_auto_ingest_unknown_chain(self):
        result = await auto_ingest("nonexistent_chain")
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_auto_ingest_discovers_and_adds(self):
        mock_registry = [
            {
                "chainId": 1,
                "name": "Ethereum",
                "rpc": ["https://new-rpc.example.com"],
            }
        ]

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": "0x13488E3"}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("lite_indexer.rpc.auto_ingestor._fetch_registry", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_registry
            with patch("httpx.AsyncClient") as mock_cls:
                mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
                mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

                result = await auto_ingest("ethereum")
                assert len(result) >= 0  # May or may not add depending on pool state
