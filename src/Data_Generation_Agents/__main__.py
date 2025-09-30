# synthetic_data_pipeline\__main__.py
import asyncio
import sys

def main():
    """Entry point for CLI."""
    from .main import main as pipeline_main
    try:
        asyncio.run(pipeline_main())
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user.")
        sys.exit(1)
    except Exception:
        sys.exit(1)

if __name__ == "__main__":
    main()