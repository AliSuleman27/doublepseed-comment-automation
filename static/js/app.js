const ARCHETYPES = {
  personal_testimony:  { label: 'Personal Testimony',  pattern: '"I used to X. {Brand} ended that."' },
  situational_react:   { label: 'Situational React',   pattern: '"the X part is too real. {Brand} fixed that for me"' },
  impulsive_action:    { label: 'Impulsive Action',    pattern: '"downloading {brand} rn because of this"' },
  social_sharing:      { label: 'Social Sharing',      pattern: '"sending this to every X I know"' },
  comparative_real:    { label: 'Comparative Real',    pattern: '"I\'ve been doing X like a caveman"' },
  hype_validation:     { label: 'Hype Validation',     pattern: '"{Brand} for X is actually genius"' },
  curiosity_question:  { label: 'Curiosity Question',  pattern: '"does {brand} actually do X?"' },
  feigned_ignorance:   { label: 'Feigned Ignorance',   pattern: '"what app is she talking about??"' },
};

const TEMPLATE_WEIGHTS = {
  'clickup_9to5': {
    personal_testimony: 20, situational_react: 20, impulsive_action: 20,
    social_sharing: 10, comparative_real: 10, hype_validation: 15,
    curiosity_question: 5, feigned_ignorance: 0,
  },
  'benjamin_slide': {
    personal_testimony: 5, situational_react: 15, impulsive_action: 15,
    social_sharing: 10, comparative_real: 5, hype_validation: 10,
    curiosity_question: 25, feigned_ignorance: 15,
  },
  default: {
    personal_testimony: 15, situational_react: 15, impulsive_action: 15,
    social_sharing: 10, comparative_real: 10, hype_validation: 15,
    curiosity_question: 15, feigned_ignorance: 5,
  },
};

const App = {
  templates: {},
  posts: [],
  products: [],
  selectedProduct: null,
  selectedTemplate: null,

  // Comment engine state
  cePosts: [],
  ceComments: {},
  ceArchetypes: {},
  ceStage: 'select',

  async init() {
    console.log('[DS] init');

    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    document.getElementById('startDate').value = this.formatDate(yesterday);
    document.getElementById('endDate').value = this.formatDate(today);
    document.getElementById('ceDate').value = this.formatDate(yesterday);

    const sidebarItems = document.querySelectorAll('.sb-item[data-pid]');
    if (sidebarItems.length > 0) {
      this.products = Array.from(sidebarItems).map(el => ({
        id: el.dataset.pid,
        title: el.querySelector('span:last-child').textContent.trim()
      }));
      this.setStatus('ready');
    } else {
      await this.loadProducts();
    }

    const lastPid = localStorage.getItem('ds_product');
    const lastTid = localStorage.getItem('ds_template');
    const validPid = this.products.find(p => p.id === lastPid) ? lastPid :
                     (this.products.length > 0 ? this.products[0].id : null);

    if (validPid) {
      await this.selectProduct(validPid);
      if (lastTid) {
        const tsel = document.getElementById('templateSelect');
        if (tsel.querySelector(`option[value="${lastTid}"]`)) {
          tsel.value = lastTid;
          this.selectedTemplate = lastTid;
        }
      }
    }
  },

  formatDate(date) {
    return date.toISOString().split('T')[0];
  },

  // ─── Tab Switching ─────────────────────────────
  switchTab(tab) {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    document.getElementById('viewPosts').classList.toggle('hidden', tab !== 'posts');
    document.getElementById('viewComments').classList.toggle('hidden', tab !== 'comments');

    // Sync product selection to comment engine
    if (tab === 'comments' && this.selectedProduct) {
      const ceSel = document.getElementById('ceProductSelect');
      ceSel.value = this.selectedProduct;
      this.ceLoadTemplates(this.selectedProduct);
    }
  },

  // ─── Products (shared) ─────────────────────────
  async loadProducts() {
    this.setStatus('loading');
    try {
      const res = await fetch('/api/products');
      const data = await res.json();

      if (data.error) { this.showWarning(data.error); this.setStatus('error'); return; }

      if (Array.isArray(data) && data.length > 0) {
        this.products = data;
        this.renderSidebar(data);
        this.renderProductDropdown(data);
        this.setStatus('ready');
      } else {
        const testRes = await fetch('/api/test');
        const diag = await testRes.json();
        this.showWarning(diag.ok ? 'No managed clients found.' : diag.message);
        this.setStatus('error');
      }
    } catch (e) {
      console.error('[DS] Failed to load products:', e);
      this.showWarning('Failed to connect to server: ' + e.message);
      this.setStatus('error');
    }
  },

  renderSidebar(products) {
    const sidebar = document.getElementById('sidebarList');
    const empty = document.getElementById('sidebarEmpty');
    if (empty) empty.remove();
    sidebar.querySelectorAll('.sb-item').forEach(el => el.remove());
    products.forEach(p => {
      const div = document.createElement('div');
      div.className = 'sb-item';
      div.dataset.pid = p.id;
      div.onclick = () => App.selectProduct(p.id);
      div.innerHTML = `<span class="icon">◈</span><span>${this.esc(p.title)}</span>`;
      sidebar.appendChild(div);
    });
  },

  renderProductDropdown(products) {
    const sel = document.getElementById('productSelect');
    const ceSel = document.getElementById('ceProductSelect');
    const opts = '<option value="">Select client...</option>' +
      products.map(p => `<option value="${p.id}">${this.esc(p.title)}</option>`).join('');
    sel.innerHTML = opts;
    ceSel.innerHTML = opts;
  },

  showWarning(msg) {
    document.getElementById('connWarningMsg').textContent = msg;
    document.getElementById('connWarning').classList.remove('hidden');
  },

  setStatus(state) {
    const dot = document.getElementById('statusDot');
    const txt = document.getElementById('statusText');
    if (state === 'ready')   { dot.style.background = 'var(--green)';  txt.textContent = 'Ready'; }
    else if (state === 'loading') { dot.style.background = 'var(--orange)'; txt.textContent = 'Loading...'; }
    else if (state === 'error')   { dot.style.background = 'var(--red)';    txt.textContent = 'Connection Issue'; }
  },

  async selectProduct(id) {
    if (!id) return;
    this.selectedProduct = id;
    localStorage.setItem('ds_product', id);
    document.querySelectorAll('.sb-item[data-pid]').forEach(el =>
      el.classList.toggle('active', el.dataset.pid === id));
    document.getElementById('productSelect').value = id;
    document.getElementById('ceProductSelect').value = id;
    await this.loadTemplates(id);
    await this.ceLoadTemplates(id);
  },

  // ─── Post Viewer: Templates ────────────────────
  async loadTemplates(productId) {
    const tsel = document.getElementById('templateSelect');
    const fetchBtn = document.getElementById('fetchBtn');

    if (this.templates[productId]) {
      this.renderTemplateDropdown(this.templates[productId]);
      return;
    }

    tsel.innerHTML = '<option value="">Loading templates...</option>';
    tsel.disabled = true;
    fetchBtn.disabled = true;

    try {
      const res = await fetch(`/api/products/${productId}/templates`);
      const data = await res.json();
      if (data.error) { tsel.innerHTML = '<option value="">Error: ' + data.error + '</option>'; return; }
      this.templates[productId] = data;
      this.renderTemplateDropdown(data);
    } catch (e) {
      console.error('[DS] template load failed:', e);
      tsel.innerHTML = '<option value="">Error loading templates</option>';
    }
  },

  renderTemplateDropdown(templates) {
    const tsel = document.getElementById('templateSelect');
    const totalPosts = templates.reduce((sum, t) => sum + (t.post_count || 0), 0);
    tsel.innerHTML = '<option value="">All templates (' + templates.length + ') — ' + totalPosts + ' posts</option>' +
      templates.map(t => `<option value="${t.id}">${this.esc(t.title || 'Template ' + t.id.slice(0, 8))}</option>`).join('');
    tsel.disabled = false;
    document.getElementById('fetchBtn').disabled = false;
  },

  onProductChange(id) { if (id) this.selectProduct(id); },

  onTemplateChange() {
    const tid = document.getElementById('templateSelect').value || null;
    this.selectedTemplate = tid;
    if (tid) localStorage.setItem('ds_template', tid); else localStorage.removeItem('ds_template');
  },

  // ─── Post Viewer: Fetch & Render ──────────────
  async fetchPosts() {
    const pid = this.selectedProduct;
    if (!pid) return;
    const tid = document.getElementById('templateSelect').value || null;
    const startDate = document.getElementById('startDate').value || null;
    const endDate = document.getElementById('endDate').value || null;

    document.getElementById('fetchBtn').disabled = true;
    document.getElementById('fetchStatus').textContent = 'Fetching...';
    this.setStatus('loading');

    try {
      const body = { product_id: pid, template_id: tid };
      if (startDate) body.start_date = startDate + 'T00:00:00Z';
      if (endDate) body.end_date = endDate + 'T23:59:59Z';

      const res = await fetch('/api/posts/fetch', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (data.error) { document.getElementById('fetchStatus').textContent = data.error; this.setStatus('error'); return; }

      this.posts = data.posts;
      this.renderStats(data);
      this.renderPosts(data.posts);
      document.getElementById('fetchStatus').textContent = `${data.count} posts found`;
      document.getElementById('exportBtn').classList.toggle('hidden', data.count === 0);
      this.setStatus('ready');
    } catch (e) {
      document.getElementById('fetchStatus').textContent = 'Error: ' + e.message;
      this.setStatus('error');
    } finally {
      document.getElementById('fetchBtn').disabled = false;
    }
  },

  renderStats(data) {
    const row = document.getElementById('statsRow');
    const accounts = new Set(data.posts.map(p => p.account_username));
    const withText = data.posts.filter(p => p.slide_texts && p.slide_texts.length > 0);
    const withLink = data.posts.filter(p => p.tiktok_url);
    row.innerHTML = `
      <div class="stat"><div class="stat-val">${data.count}</div><div class="stat-lbl">Posts</div></div>
      <div class="stat"><div class="stat-val">${accounts.size}</div><div class="stat-lbl">Accounts</div></div>
      <div class="stat"><div class="stat-val">${withText.length}</div><div class="stat-lbl">With Slide Text</div></div>
      <div class="stat"><div class="stat-val">${withLink.length}</div><div class="stat-lbl">With TikTok Link</div></div>
    `;
    row.classList.remove('hidden');
  },

  renderPosts(posts) {
    const card = document.getElementById('postsCard');
    const container = document.getElementById('postsContainer');
    document.getElementById('postsSub').textContent = `${posts.length} succeeded slideshow posts`;

    if (!posts.length) {
      container.innerHTML = '<div style="padding:24px; text-align:center; color:var(--text-3);">No posts found for this selection.</div>';
      card.classList.remove('hidden');
      return;
    }

    container.innerHTML = posts.map((p, i) => {
      const texts = (p.slide_texts || []).map((t, j) =>
        `<div class="slide-text"><span class="slide-num">S${j + 1}</span>${this.esc(t)}</div>`
      ).join('');
      const link = p.tiktok_url
        ? `<a href="${p.tiktok_url}" target="_blank" class="post-link">${p.tiktok_url}</a>`
        : '<span style="color:var(--text-3);">No link</span>';
      const time = p.created_at ? new Date(p.created_at).toLocaleString() : '—';

      return `
        <div class="post-row fade-in" style="animation-delay:${Math.min(i * 20, 400)}ms">
          <div class="post-header">
            <div class="post-acct">@${this.esc(p.account_username)}</div>
            <div class="post-meta">${p.num_slides || '?'} slides · ${time}</div>
          </div>
          <div>
            ${p.template_name ? `<span class="post-template">${this.esc(p.template_name)}</span>` : ''}
            <span class="post-type">slideshow</span>
            <span class="post-type" style="color:var(--green);background:var(--green-dim);">succeeded</span>
          </div>
          ${p.hook ? `<div class="post-hook">${this.esc(p.hook)}</div>` : ''}
          <div class="post-caption">${this.esc(p.caption || p.title || '')}</div>
          <div class="post-link-row">${link}</div>
          ${texts ? `<div class="slide-texts-container">${texts}</div>` : '<div class="no-text">No slide text extracted</div>'}
        </div>
      `;
    }).join('');
    card.classList.remove('hidden');
  },

  async exportCSV() {
    try {
      const res = await fetch('/api/posts/export', { method: 'POST' });
      const blob = await res.blob();
      const cd = res.headers.get('Content-Disposition') || '';
      const match = cd.match(/filename=(.+)/);
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = match ? match[1] : 'posts_export.csv';
      a.click();
    } catch (e) { console.error(e); }
  },

  // ═══════════════════════════════════════════════
  // COMMENT ENGINE
  // ═══════════════════════════════════════════════

  ceOnProductChange(id) {
    if (!id) return;
    this.selectedProduct = id;
    localStorage.setItem('ds_product', id);
    document.querySelectorAll('.sb-item[data-pid]').forEach(el =>
      el.classList.toggle('active', el.dataset.pid === id));
    document.getElementById('productSelect').value = id;
    this.ceLoadTemplates(id);
  },

  async ceLoadTemplates(productId) {
    const tsel = document.getElementById('ceTemplateSelect');
    if (!productId) return;

    if (this.templates[productId]) {
      this.ceRenderTemplateDropdown(this.templates[productId]);
      return;
    }

    tsel.innerHTML = '<option value="">Loading...</option>';
    tsel.disabled = true;

    try {
      const res = await fetch(`/api/products/${productId}/templates`);
      const data = await res.json();
      if (data.error) { tsel.innerHTML = '<option value="">Error</option>'; return; }
      this.templates[productId] = data;
      this.ceRenderTemplateDropdown(data);
    } catch (e) {
      tsel.innerHTML = '<option value="">Error</option>';
    }
  },

  ceRenderTemplateDropdown(templates) {
    const tsel = document.getElementById('ceTemplateSelect');
    tsel.innerHTML = '<option value="">Select template...</option>' +
      templates.map(t => `<option value="${t.id}">${this.esc(t.title || 'Template ' + t.id.slice(0, 8))}</option>`).join('');
    tsel.disabled = false;
    document.getElementById('ceLoadBtn').disabled = false;
  },

  ceOnTemplateChange() {},

  // ─── Load Posts for Comment Engine ────────────
  async ceLoadPosts() {
    const pid = document.getElementById('ceProductSelect').value;
    const tid = document.getElementById('ceTemplateSelect').value || null;
    const date = document.getElementById('ceDate').value;
    if (!pid) return;

    document.getElementById('ceLoadBtn').disabled = true;
    document.getElementById('ceLoadStatus').textContent = 'Fetching posts...';
    this.setStatus('loading');

    try {
      const body = { product_id: pid, template_id: tid };
      if (date) {
        body.start_date = date + 'T00:00:00Z';
        body.end_date = date + 'T23:59:59Z';
      }

      const res = await fetch('/api/posts/fetch', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (data.error) {
        document.getElementById('ceLoadStatus').textContent = data.error;
        this.setStatus('error');
        return;
      }

      // Only keep posts with tiktok links
      this.cePosts = data.posts.filter(p => p.tiktok_url);
      this.ceComments = {};
      this.ceArchetypes = {};

      this.ceSetStage('select');
      this.ceRenderStats();
      this.ceRenderPosts();

      const total = data.posts.length;
      const linked = this.cePosts.length;
      document.getElementById('ceLoadStatus').textContent =
        `${linked} posts with TikTok links (${total - linked} skipped without links)`;
      this.setStatus('ready');

      document.getElementById('ceBatchActions').classList.remove('hidden');
      const batchSize = parseInt(document.getElementById('ceBatchSize').value);
      const batches = Math.ceil(linked / batchSize);
      document.getElementById('ceBatchSub').textContent =
        `${linked} posts · ${batches} batch${batches !== 1 ? 'es' : ''} of ${batchSize}`;

    } catch (e) {
      document.getElementById('ceLoadStatus').textContent = 'Error: ' + e.message;
      this.setStatus('error');
    } finally {
      document.getElementById('ceLoadBtn').disabled = false;
    }
  },

  ceSetStage(stage) {
    this.ceStage = stage;
    const stages = ['select', 'archetype', 'generate', 'validate', 'export'];
    const stageIdx = stages.indexOf(stage);
    document.querySelectorAll('.pipe-stage').forEach((el, i) => {
      el.classList.remove('active', 'done');
      if (i < stageIdx) el.classList.add('done');
      else if (i === stageIdx) el.classList.add('active');
    });
  },

  ceRenderStats() {
    const row = document.getElementById('ceStatsRow');
    const posts = this.cePosts;
    const accounts = new Set(posts.map(p => p.account_username));
    const batchSize = parseInt(document.getElementById('ceBatchSize').value);
    const batches = Math.ceil(posts.length / batchSize);
    const model = document.getElementById('ceModel').selectedOptions[0].text;

    row.innerHTML = `
      <div class="stat"><div class="stat-val">${posts.length}</div><div class="stat-lbl">Posts</div></div>
      <div class="stat"><div class="stat-val">${accounts.size}</div><div class="stat-lbl">Accounts</div></div>
      <div class="stat"><div class="stat-val">${batches}</div><div class="stat-lbl">Batches</div></div>
      <div class="stat"><div class="stat-val">~${(batches * 2200).toLocaleString()}</div><div class="stat-lbl">Est. Tokens</div></div>
      <div class="stat"><div class="stat-val">${model}</div><div class="stat-lbl">Model</div></div>
    `;
    row.classList.remove('hidden');
  },

  ceRenderPosts() {
    const container = document.getElementById('cePostsList');
    const posts = this.cePosts;
    const batchSize = parseInt(document.getElementById('ceBatchSize').value);

    if (!posts.length) {
      container.innerHTML = '<div class="card" style="text-align:center; color:var(--text-3); padding:40px;">No posts with TikTok links found.</div>';
      container.classList.remove('hidden');
      return;
    }

    let html = '';
    posts.forEach((p, i) => {
      // Batch divider
      if (i % batchSize === 0) {
        const batchNum = Math.floor(i / batchSize) + 1;
        const batchEnd = Math.min(i + batchSize, posts.length);
        html += `
          <div class="batch-divider">
            <div class="batch-divider-line"></div>
            <div class="batch-divider-label">BATCH ${batchNum} · Posts ${i + 1}–${batchEnd}</div>
            <div class="batch-divider-line"></div>
          </div>
        `;
      }

      const arch = this.ceArchetypes[p.id];
      const comment = this.ceComments[p.id];
      const hasComment = !!comment;
      const slides = (p.slide_texts || []).map((t, j) =>
        `<div class="slide-text"><span class="slide-num">S${j + 1}</span>${this.esc(t)}</div>`
      ).join('');

      html += `
        <div class="ce-post-card fade-in ${hasComment ? 'has-comment' : ''}" style="animation-delay:${Math.min(i * 30, 500)}ms" data-postid="${p.id}">
          <div class="ce-post-top">
            <div class="ce-post-info">
              <div class="ce-post-num">POST ${i + 1}</div>
              <div class="ce-post-acct">@${this.esc(p.account_username)}</div>
              ${p.template_name ? `<span class="post-template" style="margin-top:4px">${this.esc(p.template_name)}</span>` : ''}
            </div>
            <a href="${p.tiktok_url}" target="_blank" class="ce-post-link">${p.tiktok_url}</a>
          </div>

          ${p.hook ? `<div class="ce-post-hook">${this.esc(p.hook)}</div>` : ''}

          ${slides ? `<div class="ce-post-slides">${slides}</div>` : ''}

          <div class="archetype-row">
            ${arch
              ? `<span class="archetype-badge arch-${arch.type}">${ARCHETYPES[arch.type].label}</span>
                 <span class="brand-mention-badge ${arch.brandMention ? 'yes' : ''}">${arch.brandMention ? 'Mention brand' : 'No brand mention'}</span>`
              : '<span style="font-size:12px; color:var(--text-3);">Archetype not assigned</span>'
            }
          </div>

          <div class="ce-comment-box ${comment ? (comment.valid ? 'valid' : 'invalid') : 'pending'}">
            <div class="ce-comment-label">
              <span>Generated Comment</span>
              ${comment ? `<span style="font-size:10px; color:${comment.valid ? 'var(--green)' : 'var(--red)'};">${comment.valid ? 'VALID' : 'FAILED'}</span>` : ''}
            </div>
            ${comment
              ? `<div class="ce-comment-text">"${this.esc(comment.text)}"</div>`
              : '<div class="ce-comment-placeholder">Awaiting generation...</div>'
            }
            ${comment && comment.checks ? `
              <div class="validation-row">
                ${comment.checks.map(c => `<span class="val-badge val-${c.status}">${c.label}</span>`).join('')}
              </div>
            ` : ''}
          </div>
        </div>
      `;
    });

    container.innerHTML = html;
    container.classList.remove('hidden');
  },

  // ─── Archetype Assignment ────────────────────
  ceAssignArchetypes() {
    const posts = this.cePosts;
    if (!posts.length) return;

    // Determine which weight profile to use
    const templateName = (posts[0].template_name || '').toLowerCase();
    let weightKey = 'default';
    if (templateName.includes('9') && templateName.includes('5')) weightKey = 'clickup_9to5';
    else if (templateName.includes('benjamin')) weightKey = 'benjamin_slide';
    const weights = TEMPLATE_WEIGHTS[weightKey];

    const batchSize = parseInt(document.getElementById('ceBatchSize').value);
    this.ceArchetypes = {};

    // Process per batch: max 2 of same archetype per batch
    for (let b = 0; b < posts.length; b += batchSize) {
      const batch = posts.slice(b, b + batchSize);
      const typeCounts = {};

      batch.forEach(p => {
        const arch = this._pickArchetype(weights, typeCounts);
        typeCounts[arch] = (typeCounts[arch] || 0) + 1;

        // Brand mention: 80% for clickup, 0% for feigned_ignorance
        const brandMention = arch === 'feigned_ignorance' ? false : Math.random() < 0.8;

        this.ceArchetypes[p.id] = { type: arch, brandMention };
      });
    }

    this.ceSetStage('archetype');
    this.ceRenderPosts();
    document.getElementById('ceGenerateBtn').disabled = false;
  },

  _pickArchetype(weights, typeCounts) {
    const available = {};
    let totalWeight = 0;
    for (const [type, weight] of Object.entries(weights)) {
      if (weight <= 0) continue;
      if ((typeCounts[type] || 0) >= 2) continue; // max 2 per batch
      available[type] = weight;
      totalWeight += weight;
    }

    // Fallback if all at max
    if (totalWeight === 0) {
      const types = Object.keys(weights).filter(t => weights[t] > 0);
      return types[Math.floor(Math.random() * types.length)];
    }

    let rand = Math.random() * totalWeight;
    for (const [type, weight] of Object.entries(available)) {
      rand -= weight;
      if (rand <= 0) return type;
    }
    return Object.keys(available)[0];
  },

  // ─── Generate Comments (placeholder) ─────────
  async ceGenerate() {
    this.ceSetStage('generate');
    document.getElementById('ceGenerateBtn').disabled = true;
    document.getElementById('ceGenerateBtn').textContent = 'Generating...';
    document.getElementById('ceGenerateBtn').classList.add('generating');

    // Simulate generation with placeholder comments per archetype
    const placeholders = {
      personal_testimony: [
        "I used to track everything in my head and now {brand} does it for me honestly life changing",
        "my whole workflow was sticky notes before {brand} ended that real quick",
        "I used to spiral every monday morning trying to remember what I had due {brand} fixed that",
      ],
      situational_react: [
        "the part about checking slack at 11pm is too real {brand} actually helped me stop doing that",
        "the morning routine part hit different because {brand} literally saved my mornings",
        "ok the overwhelmed part called me out but {brand} fixed that for me ngl",
      ],
      impulsive_action: [
        "downloading {brand} rn because this is exactly what I needed",
        "ok I just signed up for {brand} because of this vid thank you",
        "me running to download {brand} after seeing this",
      ],
      social_sharing: [
        "sending this to my entire team with a {brand} link attached",
        "forwarding this to every overwhelmed person I know tbh",
        "my manager needs to see this I'm sending it with the {brand} link rn",
      ],
      comparative_real: [
        "I've been using spreadsheets like a caveman this whole time {brand} time I guess",
        "the fact that I was doing all this manually when {brand} exists is embarrassing",
        "I feel called out because I was literally doing this the hard way before {brand}",
      ],
      hype_validation: [
        "{brand} for project management is actually genius never going back",
        "the way {brand} handles this is lowkey incredible",
        "{brand} understanding how real people actually work is the content I needed",
      ],
      curiosity_question: [
        "does {brand} actually let you do all of this from one place",
        "wait can {brand} really replace all my other apps for this",
        "is {brand} actually as good as this makes it look",
      ],
      feigned_ignorance: [
        "what app is she using for all of this I need it",
        "can someone drop the app name because I need this yesterday",
        "wait what is that app at the end someone tell me",
      ],
    };

    // Simulate a delay then assign comments
    await new Promise(r => setTimeout(r, 1500));

    const clientName = this.products.find(p => p.id === this.selectedProduct)?.title || 'the app';
    const brandName = clientName.split(' ')[0]; // e.g., "ClickUp"

    this.cePosts.forEach(p => {
      const arch = this.ceArchetypes[p.id];
      if (!arch) return;

      const templates = placeholders[arch.type] || placeholders.personal_testimony;
      let text = templates[Math.floor(Math.random() * templates.length)];
      text = text.replace(/\{brand\}/g, arch.brandMention ? brandName : '').replace(/  +/g, ' ').trim();

      // Simulate validation
      const wordCount = text.split(/\s+/).length;
      const checks = [
        { label: `${wordCount} words`, status: wordCount >= 6 && wordCount <= 25 ? 'pass' : 'fail' },
        { label: 'No ad language', status: 'pass' },
        { label: 'No emoji', status: 'pass' },
        { label: arch.brandMention ? 'Brand mentioned' : 'No brand', status: 'pass' },
        { label: 'Unique', status: 'pass' },
      ];
      const valid = checks.every(c => c.status === 'pass');

      this.ceComments[p.id] = { text, valid, checks, archetype: arch.type };
    });

    document.getElementById('ceGenerateBtn').textContent = 'Generate Comments';
    document.getElementById('ceGenerateBtn').classList.remove('generating');
    document.getElementById('ceGenerateBtn').disabled = false;
    document.getElementById('ceValidateBtn').disabled = false;
    document.getElementById('ceExportBtn').disabled = false;

    this.ceSetStage('validate');
    this.ceRenderPosts();
  },

  ceValidate() {
    // Re-run validation on all comments (placeholder — already validated in generate)
    this.ceSetStage('validate');
    this.ceRenderPosts();
  },

  ceExport() {
    this.ceSetStage('export');

    const rows = [['post_index', 'account_username', 'tiktok_url', 'archetype', 'brand_mention', 'comment', 'valid']];
    this.cePosts.forEach((p, i) => {
      const arch = this.ceArchetypes[p.id];
      const comment = this.ceComments[p.id];
      rows.push([
        i + 1,
        p.account_username,
        p.tiktok_url || '',
        arch ? arch.type : '',
        arch ? (arch.brandMention ? 'yes' : 'no') : '',
        comment ? comment.text : '',
        comment ? (comment.valid ? 'yes' : 'no') : '',
      ]);
    });

    const csv = rows.map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `comments_${this.formatDate(new Date())}.csv`;
    a.click();
  },

  // ─── Util ─────────────────────────────────────
  esc(s) {
    if (!s) return '';
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  },
};

document.addEventListener('DOMContentLoaded', () => App.init());
