    /**
 * Check if the page content has been loaded completely.
 */
function treefunc(t) {
        /**
         * Initialize the jsTree instance on the HTML content.
         * Display the tree view.
         */
        //console.log(t)
        $("#tree-view-from-json").jstree({
            core: {
                /**
                 * Add the JSON content inside the "data" attribute.
                 */
                data: [
                    t
        ]
            },
        });
};

