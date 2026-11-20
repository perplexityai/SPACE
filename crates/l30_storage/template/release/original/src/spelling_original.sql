NOT EXISTS (
    SELECT 1
    FROM template_releases spelling_elder
    WHERE spelling_elder.template_name = {release}.template_name
      AND spelling_elder.hash = {release}.hash
      AND (spelling_elder.created_at, spelling_elder.id)
          < ({release}.created_at, {release}.id))
