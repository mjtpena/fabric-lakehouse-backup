# Contributing to Microsoft Fabric Lakehouse Backup Tools

Thank you for your interest in contributing to this project! We welcome contributions from the community.

## 🤝 How to Contribute

### Reporting Issues
- Use GitHub Issues to report bugs or request features
- Provide detailed information about your environment
- Include error messages and logs when applicable
- Use the issue templates when available

### Submitting Changes
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Test your changes thoroughly
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📋 Development Guidelines

### Code Style
- Follow PEP 8 for Python code
- Use meaningful variable names
- Add comments for complex logic
- Keep functions focused and single-purpose

### Notebook Development
- Test notebooks in Microsoft Fabric environment
- Ensure notebooks work with different data sizes
- Include error handling and logging
- Document configuration parameters clearly

### Documentation
- Update README.md for new features
- Add examples for new functionality
- Keep documentation current with code changes
- Use clear, concise language

## 🧪 Testing

### Before Submitting
- Test with small and large datasets
- Verify backup and restore cycles
- Test with different storage types
- Check error handling scenarios

### Test Environments
- Microsoft Fabric workspace
- Various lakehouse sizes
- Different file types and structures
- Multiple storage authentication methods

## 📝 Pull Request Process

1. **Description**: Provide a clear description of changes
2. **Testing**: Include test results and scenarios covered
3. **Documentation**: Update relevant documentation
4. **Breaking Changes**: Clearly mark any breaking changes
5. **Review**: Be responsive to review feedback

## 🏗️ Project Structure

```
fabric-lakehouse-backup/
├── *.ipynb                 # Notebook files
├── README.md              # Main documentation
├── docs/                  # Additional documentation
├── examples/              # Usage examples
└── scripts/               # Helper scripts
```

## 🎯 Contribution Areas

We especially welcome contributions in these areas:

### Core Features
- Performance optimizations
- Additional storage backends
- Enhanced error handling
- Memory usage improvements

### Documentation
- More usage examples
- Troubleshooting guides
- Video tutorials
- Best practices documentation

### Testing
- Automated testing scripts
- Edge case scenarios
- Performance benchmarks
- Compatibility testing

### Tools & Scripts
- Validation utilities
- Migration helpers
- Monitoring scripts
- Configuration generators

## 🐛 Bug Reports

When reporting bugs, please include:
- Microsoft Fabric environment details
- Notebook runtime information
- Complete error messages
- Steps to reproduce
- Expected vs actual behavior
- Sample data (if safe to share)

## 💡 Feature Requests

For feature requests, please provide:
- Clear use case description
- Expected behavior
- Alternative solutions considered
- Impact on existing functionality

## 📞 Getting Help

- **Questions**: Use GitHub Discussions
- **Issues**: Use GitHub Issues
- **Documentation**: Check the `docs/` folder first

## 🏆 Recognition

Contributors will be recognized in:
- README.md contributors section
- Release notes for significant contributions
- Project documentation

## 📄 Code of Conduct

### Our Pledge
We pledge to make participation in our project a harassment-free experience for everyone, regardless of age, body size, disability, ethnicity, gender identity and expression, level of experience, nationality, personal appearance, race, religion, or sexual identity and orientation.

### Our Standards
- Using welcoming and inclusive language
- Being respectful of differing viewpoints
- Gracefully accepting constructive criticism
- Focusing on what is best for the community
- Showing empathy towards other community members

### Enforcement
Instances of abusive, harassing, or otherwise unacceptable behavior may be reported by contacting the project team. All complaints will be reviewed and investigated promptly and fairly.

## 📋 Development Setup

### Prerequisites
- Microsoft Fabric workspace access
- PySpark/Jupyter environment for testing
- Git for version control

### Local Development
1. Clone the repository
2. Create a test workspace in Microsoft Fabric
3. Upload notebooks to test environment
4. Make changes and test thoroughly
5. Submit pull request

## 🚀 Release Process

### Versioning
We use semantic versioning (SemVer):
- **Major**: Breaking changes
- **Minor**: New features (backward compatible)
- **Patch**: Bug fixes

### Release Cycle
- Regular maintenance releases
- Feature releases based on community needs
- Security updates as needed

Thank you for contributing to Microsoft Fabric Lakehouse Backup Tools! 🎉
