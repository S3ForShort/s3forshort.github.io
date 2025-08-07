// main.js

async function loadModules() {
  const res = await fetch("training_winners_data.json");
  const data = await res.json();
  const modules = data.packages;

  // Render top users and universities
  renderRankings("top-users", data.users, "user");
  renderRankings("top-universities", data.universities, "school");

  // Group modules by difficulty
  const grouped = {};
  modules.forEach(pkg => {
    const diff = pkg.difficulty || "Uncategorized";
    if (!grouped[diff]) grouped[diff] = [];
    grouped[diff].push(pkg);
  });

  const pointsPerLevel = data.difficulty_points || {
    "foundational": 50,
    "intermediate": 100,
    "advanced": 300
  };

  const container = document.getElementById("difficulty-columns");
  ["foundational", "intermediate", "advanced"].forEach(difficulty => {
    const packs = grouped[difficulty];
    if (!packs) return;

    const column = document.createElement("div");
    column.className = "column";
    const title = difficulty.charAt(0).toUpperCase() + difficulty.slice(1);
    const points = pointsPerLevel[difficulty] || "?";

    column.innerHTML = `<h3>${title} <span style="font-weight: normal; font-size: 0.85em; color: #d8d8d8;">(${points} pts)</span></h3>`;
    packs.forEach(pkg => column.appendChild(createModuleTile(pkg)));
    container.appendChild(column);
  });
}

function renderRankings(containerId, list, type) {
  const container = document.getElementById(containerId);
  const medals = ["🥇", "🥈", "🥉"];
  const sorted = [...list].sort((a, b) => b.total_points - a.total_points).slice(0, 3);

  for (let i = 0; i < 3; i++) {
    if (sorted[i]) {
      const name = sorted[i].full_name || sorted[i].university;
      const points = sorted[i].total_points;
      container.innerHTML += `
        <div class="rank-card medal-${i + 1}">
          <div>${medals[i]} ${name}</div>
          <div><span class="badge">${points} pts</span></div>
        </div>
      `;
    } else {
      container.innerHTML += `
        <div class="rank-card placeholder">
          💥 This ${type} spot is up for grabs — complete a challenge to claim it!
        </div>
      `;
    }
  }
}

function createModuleTile(pkg) {
  const container = document.createElement('div');

  const universityCount = pkg.universities?.length || 0;
  const userCount = pkg.users?.length || 0;
  const isCompleted = userCount > 0;

  container.setAttribute('data-module-name', pkg.package_name);
  container.setAttribute('data-users', JSON.stringify(pkg.users || []));
  container.setAttribute('data-universities', JSON.stringify(pkg.universities || []));


  // Apply conditional class
  container.className = isCompleted ? 'tile tile-completed' : 'tile tile-uncompleted';

  container.innerHTML = `
    <div class="tile-header" style="margin-bottom: 0.5rem;">
      <strong>
        <a href="https://portal.simspace.com/index.html#/training/catalog/structured-content-plan/${pkg.package_id}" target="_blank" style="color: #58a6ff; text-decoration: none;">
          ${pkg.package_name}
          <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" style="margin-left: 4px;" fill="#58a6ff" viewBox="0 0 24 24">
            <path d="M14 3h7v7h-2V6.41l-9.29 9.3-1.42-1.42 9.3-9.29H14V3z"/>
            <path d="M5 5h9V3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2v-9h-2v9H5V5z"/>
        </svg>
        </a>
      </strong>
    </div>

    <div style="font-size: 0.9rem; line-height: 1.4; margin-bottom: 0.75rem;">
      <p>Completion Threshold: ${pkg.passing_threshold}%</p>
      <p>Release Date: ${pkg.release_date}</p>
    </div>

    <div style="display: flex; flex-wrap: wrap; gap: 0.5rem;">
      <button class="stats-button" onclick='showStats("${pkg.package_name}", ${JSON.stringify(pkg.universities)})'>
        University Claims (${universityCount})
      </button>
      <button class="stats-button" onclick='showUserStats("${pkg.package_name}", ${JSON.stringify(pkg.users)})'>
        User Claims (${userCount})
      </button>
    </div>
  `;

  return container;
}


function showStats(name, universities) {
  const modal = document.getElementById("stats-modal");
  const title = document.getElementById("modal-title");
  const content = document.getElementById("modal-content-list");

  const count = universities.length;
  title.textContent = `${name}`;
  content.innerHTML = '';

  if (count === 0) {
    content.innerHTML = `<li>No schools have completed this module yet.</li>`;
  } else {
    universities
      .sort((a, b) => a.name.localeCompare(b.name))
      .forEach(u => {
        const li = document.createElement("li");
        li.textContent = `${u.name} — ${u.users} user${u.users > 1 ? 's' : ''}`;
        content.appendChild(li);
      });
  }

  modal.style.display = "flex";
}

function showUserStats(packageName, users) {
  const modal = document.getElementById("stats-modal");
  const title = document.getElementById("modal-title");
  const content = document.getElementById("modal-content-list");

  title.textContent = `${packageName}`;
  content.innerHTML = '';

  if (users.length === 0) {
    content.innerHTML = `<li>No users have earned points on this module yet.</li>`;
  } else {
    users
      .sort((a, b) => {
        const scoreDiff = b.challenge_points_earned - a.challenge_points_earned;
        return scoreDiff !== 0
          ? scoreDiff
          : a.full_name.localeCompare(b.full_name);
      })
      .forEach(user => {
        const li = document.createElement("li");
        li.textContent = `${user.full_name} — ${user.challenge_points_earned} pts`;
        content.appendChild(li);
      });
  }

  modal.style.display = "flex";
}

function applyModuleFilters() {
  const userQuery = document.getElementById('user-search').value.toLowerCase();
  const universityQuery = document.getElementById('university-search').value.toLowerCase();
  const moduleQuery = document.getElementById('module-search').value.toLowerCase();
  const claimFilter = document.getElementById('claimed-filter').value;

  document.querySelectorAll('.tile').forEach(tile => {
    const name = tile.getAttribute('data-module-name') || '';
    const users = JSON.parse(tile.getAttribute('data-users') || '[]');
    const universities = JSON.parse(tile.getAttribute('data-universities') || '[]');

    const isClaimed = users.length > 0;
    let visible = true;

    // Filter by module name
    if (moduleQuery && !name.toLowerCase().includes(moduleQuery)) {
      visible = false;
    }

    // Filter by user
    if (userQuery && !users.some(u => u.full_name.toLowerCase().includes(userQuery))) {
      visible = false;
    }

    // Filter by university
    if (universityQuery && !universities.some(u => u.name.toLowerCase().includes(universityQuery))) {
      visible = false;
    }

    // Filter by claimed/unclaimed
    if (claimFilter === 'claimed' && !isClaimed) visible = false;
    if (claimFilter === 'unclaimed' && isClaimed) visible = false;

    tile.style.display = visible ? '' : 'none';
  });
}


function closeModal() {
  document.getElementById("stats-modal").style.display = "none";
}

document.addEventListener("DOMContentLoaded", loadModules);

['user-search', 'university-search', 'module-search', 'claimed-filter'].forEach(id => {
  document.getElementById(id).addEventListener('input', applyModuleFilters);
});

