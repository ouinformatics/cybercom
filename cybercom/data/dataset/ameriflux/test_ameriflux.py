import nose

import ameriflux

def test_nose():
    assert 'a' == 'a'


def test_get_weekly():
    assert len(ameriflux.get_monthly( ameriflux.location, ameriflux.date_from, ameriflux.date_to, ameriflux.variable)) == 12
