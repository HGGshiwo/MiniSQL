from BufferManager import pin_page, unpin_page


def read_catalog(share):
    pin_page(share, 'catalog')
    catalog_info = share.catalog_info
    return catalog_info


def fresh_catalog(share, catalog_info):
    share.catalog_info = catalog_info
    unpin_page(share, 'catalog')