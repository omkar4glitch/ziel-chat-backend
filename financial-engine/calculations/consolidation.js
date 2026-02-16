export function buildKPI(summary){

  let worstStore = null;
  let bestStore = null;

  Object.keys(summary.stores || {}).forEach(store =>{

    const m = summary.stores[store].ebitdaMargin;

    if(!worstStore || m < worstStore.margin)
      worstStore = {store, margin:m};

    if(!bestStore || m > bestStore.margin)
      bestStore = {store, margin:m};
  });

  return {
    worstStore,
    bestStore,
    consolidated: summary.consolidated
  };
}
