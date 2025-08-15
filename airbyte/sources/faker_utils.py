"""Utility functions for Source Faker configuration validation and error handling."""

from __future__ import annotations

from typing import Any


def validate_faker_config(config: dict[str, Any]) -> dict[str, Any]:
    """
    Validate and normalize Source Faker configuration parameters.
    
    Args:
        config: Raw configuration dictionary for source-faker
        
    Returns:
        Validated and normalized configuration dictionary
        
    Raises:
        ValueError: If configuration contains invalid parameters
    """
    validated_config = config.copy()
    
    if "count" in validated_config:
        count = validated_config["count"]
        if not isinstance(count, int) or count <= 0:
            raise ValueError(
                f"Source Faker 'count' parameter must be a positive integer, got: {count}"
            )
        if count > 1000000:
            raise ValueError(
                f"Source Faker 'count' parameter is too large ({count}). "
                "Consider using a smaller value for better performance."
            )
    
    if "seed" in validated_config:
        seed = validated_config["seed"]
        if not isinstance(seed, int) or seed < 0:
            raise ValueError(
                f"Source Faker 'seed' parameter must be a non-negative integer, got: {seed}"
            )
    
    if "parallelism" in validated_config:
        parallelism = validated_config["parallelism"]
        if not isinstance(parallelism, int) or parallelism <= 0:
            raise ValueError(
                f"Source Faker 'parallelism' parameter must be a positive integer, got: {parallelism}"
            )
        if parallelism > 32:
            raise ValueError(
                f"Source Faker 'parallelism' parameter is too high ({parallelism}). "
                "Consider using a value between 1-32 for optimal performance."
            )
    
    if "always_updated" in validated_config:
        always_updated = validated_config["always_updated"]
        if not isinstance(always_updated, bool):
            raise ValueError(
                f"Source Faker 'always_updated' parameter must be a boolean, got: {always_updated}"
            )
    
    return validated_config


def get_faker_config_recommendations(config: dict[str, Any]) -> list[str]:
    """
    Get configuration recommendations for Source Faker based on the provided config.
    
    Args:
        config: Source Faker configuration dictionary
        
    Returns:
        List of recommendation strings
    """
    recommendations = []
    
    count = config.get("count", 100)
    parallelism = config.get("parallelism", 4)
    
    if count > 10000 and parallelism < 8:
        recommendations.append(
            f"For large datasets (count={count}), consider increasing parallelism "
            f"from {parallelism} to 8-16 for better performance."
        )
    
    if count < 100 and parallelism > 4:
        recommendations.append(
            f"For small datasets (count={count}), parallelism={parallelism} may be "
            "unnecessary. Consider reducing to 1-4 for simpler execution."
        )
    
    if "seed" not in config:
        recommendations.append(
            "Consider adding a 'seed' parameter for reproducible test data generation."
        )
    
    return recommendations
