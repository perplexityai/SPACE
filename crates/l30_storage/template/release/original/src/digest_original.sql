NOT EXISTS (
    SELECT 1
    FROM template_releases digest_rival
    WHERE digest_rival.template_name = {release}.template_name
      AND digest_rival.hash IN (replace({release}.hash, 'sha256:', ''),
                                'sha256:' || replace({release}.hash, 'sha256:', ''))
      AND ((digest_rival.hash = {release}.hash
            AND (digest_rival.created_at, digest_rival.id)
                < ({release}.created_at, {release}.id))
           OR (digest_rival.hash <> {release}.hash
               AND NOT EXISTS (
                   SELECT 1
                   FROM template_releases digest_rival_elder
                   WHERE digest_rival_elder.template_name = digest_rival.template_name
                     AND digest_rival_elder.hash = digest_rival.hash
                     AND (digest_rival_elder.created_at, digest_rival_elder.id)
                         < (digest_rival.created_at, digest_rival.id))
               AND ((EXISTS (
                         SELECT 1
                         FROM template_release_storage_locations digest_rival_storage
                         WHERE digest_rival_storage.template_release_id = digest_rival.id)
                     AND NOT EXISTS (
                         SELECT 1
                         FROM template_release_storage_locations digest_own_storage
                         WHERE digest_own_storage.template_release_id = {release}.id))
                    OR (EXISTS (
                            SELECT 1
                            FROM template_release_storage_locations digest_rival_storage
                            WHERE digest_rival_storage.template_release_id = digest_rival.id)
                        = EXISTS (
                            SELECT 1
                            FROM template_release_storage_locations digest_own_storage
                            WHERE digest_own_storage.template_release_id = {release}.id)
                        AND (digest_rival.created_at, digest_rival.id)
                            < ({release}.created_at, {release}.id))))))
