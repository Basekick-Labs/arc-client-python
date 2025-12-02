"""Unit tests for auth module."""

from __future__ import annotations

from arc_client.models.auth import (
    CreateTokenResponse,
    RotateTokenResponse,
    TokenInfo,
    TokenListResponse,
    VerifyResponse,
)


class TestTokenInfo:
    """Tests for TokenInfo model."""

    def test_token_info_full(self) -> None:
        """Test TokenInfo with all fields."""
        data = {
            "id": 123,
            "name": "test-token",
            "description": "Test token description",
            "permissions": ["read", "write"],
            "created_at": "2024-01-01T00:00:00Z",
            "last_used_at": "2024-01-02T00:00:00Z",
            "enabled": True,
            "expires_at": "2024-12-31T23:59:59Z",
        }
        token = TokenInfo(**data)

        assert token.id == 123
        assert token.name == "test-token"
        assert token.description == "Test token description"
        assert token.permissions == ["read", "write"]
        assert token.enabled is True

    def test_token_info_minimal(self) -> None:
        """Test TokenInfo with minimal fields."""
        token = TokenInfo(id=1, name="minimal")

        assert token.id == 1
        assert token.name == "minimal"
        assert token.description is None
        assert token.permissions == []
        assert token.enabled is True


class TestVerifyResponse:
    """Tests for VerifyResponse model."""

    def test_verify_response_valid(self) -> None:
        """Test valid token verification response."""
        data = {
            "valid": True,
            "token_info": {
                "id": 1,
                "name": "test",
                "permissions": ["read", "write"],
            },
            "permissions": ["read", "write"],
        }
        response = VerifyResponse(**data)

        assert response.valid is True
        assert response.token_info is not None
        assert response.token_info.name == "test"
        assert response.permissions == ["read", "write"]
        assert response.error is None

    def test_verify_response_invalid(self) -> None:
        """Test invalid token verification response."""
        response = VerifyResponse(valid=False, error="Invalid token")

        assert response.valid is False
        assert response.error == "Invalid token"
        assert response.token_info is None


class TestCreateTokenResponse:
    """Tests for CreateTokenResponse model."""

    def test_create_token_success(self) -> None:
        """Test successful token creation response."""
        data = {
            "success": True,
            "token": "arc_abc123xyz",
            "message": "Token created successfully",
        }
        response = CreateTokenResponse(**data)

        assert response.success is True
        assert response.token == "arc_abc123xyz"
        assert response.message == "Token created successfully"
        assert response.error is None

    def test_create_token_failure(self) -> None:
        """Test failed token creation response."""
        response = CreateTokenResponse(success=False, error="Name already exists")

        assert response.success is False
        assert response.token is None
        assert response.error == "Name already exists"


class TestTokenListResponse:
    """Tests for TokenListResponse model."""

    def test_token_list_response(self) -> None:
        """Test token list response."""
        data = {
            "success": True,
            "tokens": [
                {"id": 1, "name": "token1", "permissions": ["read"]},
                {"id": 2, "name": "token2", "permissions": ["read", "write"]},
            ],
            "count": 2,
        }
        response = TokenListResponse(**data)

        assert response.success is True
        assert len(response.tokens) == 2
        assert response.count == 2
        assert response.tokens[0].name == "token1"
        assert response.tokens[1].permissions == ["read", "write"]

    def test_token_list_empty(self) -> None:
        """Test empty token list response."""
        response = TokenListResponse(success=True)

        assert response.success is True
        assert response.tokens == []
        assert response.count == 0


class TestRotateTokenResponse:
    """Tests for RotateTokenResponse model."""

    def test_rotate_token_success(self) -> None:
        """Test successful token rotation response."""
        data = {
            "success": True,
            "new_token": "arc_newtoken123",
            "message": "Token rotated successfully",
        }
        response = RotateTokenResponse(**data)

        assert response.success is True
        assert response.new_token == "arc_newtoken123"
        assert response.message == "Token rotated successfully"

    def test_rotate_token_failure(self) -> None:
        """Test failed token rotation response."""
        response = RotateTokenResponse(success=False, error="Token not found")

        assert response.success is False
        assert response.new_token is None
        assert response.error == "Token not found"


class TestAuthClientVerify:
    """Tests for AuthClient verify method."""

    def test_verify_no_token(self) -> None:
        """Test verify with no token configured."""
        from unittest.mock import MagicMock

        from arc_client.auth.manager import AuthClient

        http = MagicMock()
        config = MagicMock()
        config.token = None

        client = AuthClient(http, config)
        result = client.verify()

        assert result.valid is False
        assert result.error == "No token configured"
        http.get.assert_not_called()
