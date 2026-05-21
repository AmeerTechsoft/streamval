# Development

Benchmarks, release history, and contributor resources.

## In this section

| Guide | Description |
|---|---|
| [Benchmarks](benchmarks.md) | Throughput and memory measurement |
| [Changelog](changelog.md) | Version history and release notes |

## Repository

- **Source:** [github.com/AmeerTechsoft/streamval](https://github.com/AmeerTechsoft/streamval)
- **Issues:** [github.com/AmeerTechsoft/streamval/issues](https://github.com/AmeerTechsoft/streamval/issues)
- **PyPI:** [pypi.org/project/streamval](https://pypi.org/project/streamval/)

## Contributing

```bash
git clone https://github.com/AmeerTechsoft/streamval
cd streamval
pip install -e ".[dev]"
pytest
```

## Building docs locally

```bash
pip install -e ".[docs]"
mkdocs serve
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).
