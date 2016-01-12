from distutils.core import setup

setup(
    name='SubClassReasoner',
    version='0.1',
    packages=['SubClassReasoner', 'SubClassReasoner.Worker', 'SubClassReasoner.Utilities',
              'SubClassReasoner.RemoteSparql', 'SubClassReasoner.NtripleParser', 'SubClassReasoner.ProcessManager'],
    url='',
    license='',
    author='Thomas Weißgerber',
    author_email='weissger@fim.uni-passau.de',
    description='', requires=['click']
)
